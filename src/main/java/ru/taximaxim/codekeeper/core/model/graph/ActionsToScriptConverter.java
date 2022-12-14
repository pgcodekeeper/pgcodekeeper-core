package ru.taximaxim.codekeeper.core.model.graph;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import ru.taximaxim.codekeeper.core.MsDiffUtils;
import ru.taximaxim.codekeeper.core.NotAllowedObjectException;
import ru.taximaxim.codekeeper.core.PgDiffArguments;
import ru.taximaxim.codekeeper.core.PgDiffScript;
import ru.taximaxim.codekeeper.core.PgDiffUtils;
import ru.taximaxim.codekeeper.core.model.difftree.DbObjType;
import ru.taximaxim.codekeeper.core.model.difftree.TreeElement;
import ru.taximaxim.codekeeper.core.schema.AbstractColumn;
import ru.taximaxim.codekeeper.core.schema.AbstractForeignTable;
import ru.taximaxim.codekeeper.core.schema.AbstractTable;
import ru.taximaxim.codekeeper.core.schema.MsColumn;
import ru.taximaxim.codekeeper.core.schema.MsView;
import ru.taximaxim.codekeeper.core.schema.PgColumn;
import ru.taximaxim.codekeeper.core.schema.PgDatabase;
import ru.taximaxim.codekeeper.core.schema.PgSequence;
import ru.taximaxim.codekeeper.core.schema.PgStatement;

public class ActionsToScriptConverter {

    private static final String REFRESH_MODULE = "EXEC sys.sp_refreshsqlmodule {0} \nGO";

    private static final String DROP_COMMENT = "-- DEPCY: This {0} depends on the {1}: {2}";
    private static final String CREATE_COMMENT = "-- DEPCY: This {0} is a dependency of {1}: {2}";
    private static final String HIDDEN_OBJECT = "-- HIDDEN: Object {0} of type {1} (action: {2}, reason: {3})";

    private static final String RENAME_PG_OBJECT = "ALTER {0} {1} RENAME TO {2};";
    private static final String RENAME_MS_OBJECT = "EXEC sp_rename {0}, {1}\nGO";

    private final PgDiffScript script;
    private final Set<ActionContainer> actions;
    private final Set<PgStatement> toRefresh;
    private final PgDiffArguments arguments;
    private final PgDatabase oldDbFull;
    private final PgDatabase newDbFull;

    private final Set<PgSequence> sequencesOwnedBy = new LinkedHashSet<>();
    /**
     * renamed table qualified names and their temporary (simple) names
     */
    private Map<String, String> tblTmpNames;
    /**
     * old tables q-names (before rename) and their identity columns' names
     */
    private Map<String, List<String>> tblIdentityCols;

    public ActionsToScriptConverter(PgDiffScript script, Set<ActionContainer> actions,
            PgDiffArguments arguments, PgDatabase oldDbFull, PgDatabase newDbFull) {
        this(script, actions, Collections.emptySet(), arguments, oldDbFull, newDbFull);
    }

    /**
     * @param toRefresh an ordered set of refreshed statements in reverse order
     */
    public ActionsToScriptConverter(PgDiffScript script, Set<ActionContainer> actions,
            Set<PgStatement> toRefresh, PgDiffArguments arguments, PgDatabase oldDbFull, PgDatabase newDbFull) {
        this.script = script;
        this.actions = actions;
        this.arguments = arguments;
        this.toRefresh = toRefresh;
        this.oldDbFull = oldDbFull;
        this.newDbFull = newDbFull;
        if (arguments.isDataMovementMode()) {
            tblTmpNames = new HashMap<>();
            tblIdentityCols = new HashMap<>();
        }
    }

    /**
     * Fills a script with objects based on their dependency order
     *
     * @param selected
     *            list of selected elements
     */
    public void fillScript(List<TreeElement> selected) {
        Set<PgStatement> refreshed = new HashSet<>(toRefresh.size());
        for (ActionContainer action : actions) {
            PgStatement obj = action.getOldObj();

            if (toRefresh.contains(obj)) {
                if (action.getAction() == StatementActions.CREATE && obj instanceof MsView) {
                    // emit refreshes for views only
                    // refreshes for other objects serve as markers
                    // that allow us to skip unmodified drop+create pairs
                    script.addStatement(MessageFormat.format(REFRESH_MODULE,
                            PgDiffUtils.quoteString(obj.getQualifiedName())));
                    refreshed.add(obj);
                }
                continue;
            }

            if (hideAction(action, selected)) {
                continue;
            }

            processSequence(action);
            printAction(action, obj);
        }

        for (PgSequence sequence : sequencesOwnedBy) {
            String ownedBy = sequence.getOwnedBySQL();
            if (!ownedBy.isEmpty()) {
                script.addStatement(ownedBy);
            }
        }

        // As a result of discussion with the SQL database developers, it was
        // decided that, in pgCodeKeeper, refresh operations are required only
        // for MsView objects. This is why a filter is used here that only
        // leaves refresh operations for MsView objects.
        //
        // if any refreshes were not emitted as statement replacements
        // add them explicitly in reverse order (the resolver adds them in "drop order")
        PgStatement[] orphanRefreshes = toRefresh.stream()
                .filter(r -> r instanceof MsView && !refreshed.contains(r))
                .toArray(PgStatement[]::new);
        for (int i = orphanRefreshes.length - 1; i >= 0; --i) {
            script.addStatement(MessageFormat.format(REFRESH_MODULE,
                    PgDiffUtils.quoteString(orphanRefreshes[i].getQualifiedName())));
        }
    }

    private void printAction(ActionContainer action, PgStatement obj) {
        String depcy = getComment(action, obj);
        switch (action.getAction()) {
        case CREATE:
            if (depcy != null) {
                script.addStatement(depcy);
            }
            script.addCreate(obj, null, obj.getCreationSQL(), true);

            if (arguments.isDataMovementMode()
                    && DbObjType.TABLE == obj.getStatementType()
                    && !(obj instanceof AbstractForeignTable)
                    && obj.getTwin(oldDbFull) != null) {
                addCommandsForMoveData((AbstractTable) obj);
            }
            break;
        case DROP:
            if (depcy != null) {
                script.addStatement(depcy);
            }
            if (arguments.isDataMovementMode()
                    && DbObjType.TABLE == obj.getStatementType()
                    && !(obj instanceof AbstractForeignTable)
                    && obj.getTwin(newDbFull) != null) {
                addCommandsForRenameTbl((AbstractTable) obj);
            } else {
                script.addDrop(obj, null, obj.getDropSQL());
            }
            break;
        case ALTER:
            StringBuilder sb = new StringBuilder();
            obj.appendAlterSQL(action.getNewObj(), sb,
                    new AtomicBoolean());
            if (sb.length() > 0) {
                if (depcy != null) {
                    script.addStatement(depcy);
                }
                script.addStatement(sb.toString());
            }
            break;
        case NONE:
            throw new IllegalStateException("Not implemented action");
        }
    }

    private String getComment(ActionContainer action, PgStatement oldObj) {
        PgStatement objStarter = action.getStarter();
        if (objStarter == null || objStarter == oldObj || objStarter == action.getNewObj()) {
            return null;
        }

        // skip column to parent
        if (objStarter.getStatementType() == DbObjType.COLUMN
                && objStarter.getParent().equals(oldObj)) {
            return null;
        }

        return MessageFormat.format(
                action.getAction() == StatementActions.CREATE ?
                        CREATE_COMMENT : DROP_COMMENT,
                        oldObj.getStatementType(),
                        objStarter.getStatementType(),
                        objStarter.getQualifiedName());
    }

    private void processSequence(ActionContainer action) {
        if (action.getOldObj() instanceof PgSequence) {
            PgSequence oldSeq = (PgSequence) action.getOldObj();
            PgSequence newSeq = (PgSequence) action.getNewObj();
            if (newSeq.getOwnedBy() != null
                    && action.getAction() == StatementActions.CREATE
                    || (action.getAction() == StatementActions.ALTER &&
                    !Objects.equals(newSeq.getOwnedBy(), oldSeq.getOwnedBy()))) {
                sequencesOwnedBy.add(newSeq);
            }
        }
    }

    /**
     * @return true if action was hidden, false if it may be executed
     */
    private boolean hideAction(ActionContainer action, List<TreeElement> selected) {
        PgStatement obj = action.getOldObj();
        if (action.getAction() == StatementActions.DROP && !obj.canDrop()) {
            addHiddenObj(action, "object cannot be dropped");
            return true;
        }
        if (arguments.isSelectedOnly() && !isSelectedAction(action, selected)) {
            addHiddenObj(action, "cannot change unselected objects in selected-only mode");
            return true;
        }

        DbObjType type = obj.getStatementType();
        if (type == DbObjType.COLUMN) {
            type = DbObjType.TABLE;
        }
        Collection<DbObjType> allowedTypes = arguments.getAllowedTypes();
        if (!allowedTypes.isEmpty() && !allowedTypes.contains(type)) {
            if (arguments.isStopNotAllowed()) {
                throw new NotAllowedObjectException(action.getOldObj().getQualifiedName()
                        + " (" + type + ") is not an allowed script object. Stopping.");
            }
            addHiddenObj(action, "object type is not in allowed types list");
            return true;
        }

        return false;
    }

    private void addHiddenObj(ActionContainer action, String reason) {
        PgStatement old = action.getOldObj();
        String message = MessageFormat.format(HIDDEN_OBJECT,
                old.getQualifiedName(), old.getStatementType(), action.getAction(), reason);
        script.addStatement(message);
    }

    /**
     * Determines whether an action object has been selected in the diff panel.
     *
     * @param action script action element
     * @param selected collection of selected elements in diff panel
     *
     * @return TRUE if the action object was selected in the diff panel, otherwise FALSE
     */
    private boolean isSelectedAction(ActionContainer action, List<TreeElement> selected) {
        Predicate<PgStatement> isSelectedObj = obj ->
        selected.stream()
        .filter(e -> e.getType().equals(obj.getStatementType()))
        .filter(e -> e.getName().equals(obj.getName()))
        .map(e -> e.getPgStatement(obj.getDatabase()))
        .anyMatch(obj::equals);

        switch (action.getAction()) {
        case CREATE:
            return isSelectedObj.test(action.getNewObj());
        case ALTER:
            return isSelectedObj.test(action.getNewObj())
                    && isSelectedObj.test(action.getOldObj());
        case DROP:
            return isSelectedObj.test(action.getOldObj());
        default:
            throw new IllegalStateException("Not implemented action");
        }
    }

    /**
     * Adds commands to the script for rename the original table name to a
     * temporary name, given the constraints. Fills the maps {@link #tblTmpNames}
     * and {@link #tblIdentityCols} for use them later (when adding commands to
     * move data from a temporary table to a new table).
     */
    private void addCommandsForRenameTbl(AbstractTable oldTbl) {
        String tmpSuffix = '_' + UUID.randomUUID().toString().replace("-", "");
        String qname = oldTbl.getQualifiedName();
        String tmpTblName = oldTbl.getName() + tmpSuffix;

        script.addStatement(getRenameCommand(oldTbl, tmpTblName));
        tblTmpNames.put(qname, tmpTblName);

        for (AbstractColumn col : oldTbl.getColumns()) {
            if (arguments.isMsSql()) {
                MsColumn msCol = (MsColumn) col;
                if (msCol.isIdentity()) {
                    tblIdentityCols.computeIfAbsent(qname, k -> new ArrayList<>())
                    .add(msCol.getName());
                }
                if (msCol.getDefaultName() != null) {
                    script.addStatement("ALTER TABLE "
                            + MsDiffUtils.quoteName(oldTbl.getSchemaName()) + '.'
                            + MsDiffUtils.quoteName(tmpTblName) + " DROP CONSTRAINT "
                            + MsDiffUtils.quoteName(msCol.getDefaultName())
                            + PgStatement.GO);
                }
            } else {
                PgColumn pgCol = (PgColumn) col;
                if (pgCol.getSequence() != null) {
                    script.addStatement(getRenameCommand(pgCol.getSequence(),
                            pgCol.getSequence().getName() + tmpSuffix));
                    tblIdentityCols.computeIfAbsent(qname, k -> new ArrayList<>())
                    .add(pgCol.getName());
                }
            }
        }
    }

    /**
     * Returns sql command to rename the given object.
     *
     * @param st object for rename
     * @param newName the new name for given object
     * @return sql command to rename the given object
     */
    private String getRenameCommand(PgStatement st, String newName) {
        return arguments.isMsSql() ?
                MessageFormat.format(RENAME_MS_OBJECT,
                        PgDiffUtils.quoteString(st.getQualifiedName()),
                        PgDiffUtils.quoteString(newName))
                : MessageFormat.format(RENAME_PG_OBJECT, st.getStatementType(),
                        st.getQualifiedName(), PgDiffUtils.getQuotedName(newName));
    }

    /**
     * Adds commands to the script for move data from the temporary table
     * to the new table, given the identity columns, and a command to delete
     * the temporary table.
     */
    private void addCommandsForMoveData(AbstractTable newTbl) {
        AbstractTable oldTbl = (AbstractTable) newTbl.getTwin(oldDbFull);
        String tblQName = newTbl.getQualifiedName();
        String tblTmpBareName = tblTmpNames.get(tblQName);

        if (tblTmpBareName == null) {
            return;
        }

        UnaryOperator<String> quoter = arguments.isMsSql() ?
                MsDiffUtils::quoteName : PgDiffUtils::getQuotedName;
        String tblTmpQName = quoter.apply(oldTbl.getSchemaName()) + '.'
                + quoter.apply(tblTmpBareName);

        List<String> colsForMovingData = getColsForMovingData(newTbl);
        List<String> identityCols = tblIdentityCols.get(tblQName);
        List<String> identityColsForMovingData = identityCols == null ? Collections.emptyList()
                : identityCols.stream().filter(colsForMovingData::contains)
                .collect(Collectors.toList());

        StringBuilder sb = new StringBuilder();

        if (arguments.isMsSql() && !identityColsForMovingData.isEmpty()) {
            // There can only be one IDENTITY column per table in MSSQL.
            sb.append("SET IDENTITY_INSERT ").append(tblQName)
            .append(" ON").append(PgStatement.GO).append("\n\n");
        }

        String cols = colsForMovingData.stream()
                .map(quoter)
                .collect(Collectors.joining(", "));
        sb.append("INSERT INTO ").append(tblQName).append('(')
        .append(cols).append(")");
        if (!arguments.isMsSql() && identityCols != null) {
            sb.append("\nOVERRIDING SYSTEM VALUE");
        }
        sb.append("\nSELECT ").append(cols).append(" FROM ")
        .append(tblTmpQName).append(arguments.isMsSql() ? PgStatement.GO : ";");

        if (arguments.isMsSql() && !identityColsForMovingData.isEmpty()) {
            // There can only be one IDENTITY column per table in MSSQL.
            sb.append("\n\nSET IDENTITY_INSERT ").append(tblQName)
            .append(" OFF").append(PgStatement.GO);
        }

        if (!identityColsForMovingData.isEmpty()) {
            if (arguments.isMsSql()) {
                // There can only be one IDENTITY column per table in MSSQL.
                // DECLARE'd var is only visible within its batch
                // so we shouldn't need unique names for them here
                // use the largest numeric type to fit any possible identity value
                sb.append("\n\nDECLARE @restart_var numeric(38,0) = (SELECT IDENT_CURRENT (")
                .append(PgDiffUtils.quoteString(tblTmpQName))
                .append("));\nDBCC CHECKIDENT (")
                .append(PgDiffUtils.quoteString(tblQName))
                .append(", RESEED, @restart_var);")
                .append(PgStatement.GO);
            } else {
                for (String colName : identityColsForMovingData) {
                    String restartWith = " ALTER TABLE " + tblQName + " ALTER COLUMN "
                            + PgDiffUtils.getQuotedName(colName)
                            + " RESTART WITH ";
                    restartWith = PgDiffUtils.quoteStringDollar(restartWith)
                            + " || restart_var || ';'";
                    String doBody = "\nDECLARE restart_var bigint = (SELECT nextval(pg_get_serial_sequence("
                            + PgDiffUtils.quoteString(tblTmpQName) + ", "
                            + PgDiffUtils.quoteString(PgDiffUtils.getQuotedName(colName))
                            + ")));\nBEGIN\n\tEXECUTE " + restartWith + " ;\nEND;\n";
                    sb.append("\n\nDO LANGUAGE plpgsql ")
                    .append(PgDiffUtils.quoteStringDollar(doBody))
                    .append(';');
                }
            }
        }

        sb.append("\n\nDROP TABLE ").append(tblTmpQName)
        .append(arguments.isMsSql() ? PgStatement.GO : ';');

        script.addStatement(sb.toString());
    }

    /**
     * Returns the names of the columns from which data will be moved to another
     * table, excluding calculated columns.
     */
    private List<String> getColsForMovingData(AbstractTable newTbl) {
        AbstractTable oldTable = (AbstractTable) newTbl.getTwin(oldDbFull);
        Stream<? extends AbstractColumn> cols = newTbl.getColumns().stream()
                .filter(c -> oldTable.containsColumn(c.getName()));
        if (arguments.isMsSql()) {
            cols = cols.map(MsColumn.class::cast)
                    .filter(msCol -> msCol.getExpression() == null);
        } else {
            cols = cols.map(PgColumn.class::cast)
                    .filter(pgCol -> !pgCol.isGenerated());
        }
        return cols.map(AbstractColumn::getName).collect(Collectors.toList());
    }
}
