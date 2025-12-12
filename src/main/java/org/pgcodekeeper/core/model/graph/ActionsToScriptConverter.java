/*******************************************************************************
 * Copyright 2017-2025 TAXTELECOM, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.pgcodekeeper.core.model.graph;

import org.pgcodekeeper.core.*;
import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.model.difftree.TreeElement;
import org.pgcodekeeper.core.database.ms.schema.MsColumn;
import org.pgcodekeeper.core.database.ms.schema.MsConstraintPk;
import org.pgcodekeeper.core.database.ms.schema.MsTable;
import org.pgcodekeeper.core.database.ms.schema.MsView;
import org.pgcodekeeper.core.database.pg.schema.PgPartitionTable;
import org.pgcodekeeper.core.database.pg.schema.PgColumn;
import org.pgcodekeeper.core.database.pg.schema.PgSequence;
import org.pgcodekeeper.core.script.SQLScript;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.*;
import java.util.Map.Entry;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * Converts database action containers into executable SQL script statements.
 * Processes CREATE, ALTER, and DROP operations in proper dependency order while handling
 * special cases like data movement, table renaming, and partition tables.
 *
 * <p>This class is responsible for generating the final SQL migration script from
 * resolved database actions. It handles complex scenarios including:
 * <ul>
 * <li>Data movement mode with temporary table creation and renaming</li>
 * <li>Joinable column alterations that can be combined into single ALTER TABLE statements</li>
 * <li>Partition table hierarchies and their dependencies</li>
 * <li>Microsoft SQL module refresh operations</li>
 * <li>Identity column preservation during data movement</li>
 * </ul>
 *
 * <p>The converter processes actions in dependency order and applies filtering
 * based on user selections and configured object type restrictions.
 */
public final class ActionsToScriptConverter {

    private static final String REFRESH_MODULE = "EXEC sys.sp_refreshsqlmodule %s";

    private static final String DROP_COMMENT = "-- DEPCY: This %s %s depends on the %s: %s";
    private static final String CREATE_COMMENT = "-- DEPCY: This %s %s is a dependency of %s: %s";
    private static final String HIDDEN_OBJECT = "-- HIDDEN: Object %s of type %s (action: %s, reason: %s)";

    private static final String RENAME_PG_OBJECT = "ALTER %s %s RENAME TO %s";
    private static final String RENAME_MS_OBJECT = "EXEC sp_rename %s, %s";
    private static final String RENAME_CH_OBJECT = "RENAME %s %s TO %s;";

    private final SQLScript script;
    private final ISettings settings;
    private final Set<ActionContainer> actions;
    private final Set<IStatement> toRefresh;
    private final IDatabase oldDbFull;
    private final IDatabase newDbFull;

    private final Map<ActionContainer, List<ActionContainer>> joinableTableActions = new HashMap<>();
    private final Set<ActionContainer> toSkip = new HashSet<>();
    private final Set<IStatement> droppedObjects = new HashSet<>();

    /**
     * renamed table qualified names and their temporary (simple) names
     */
    private Map<String, String> tblTmpNames;
    /**
     * old tables q-names (before rename) and their identity columns' names
     */
    private Map<String, List<String>> tblIdentityCols;

    /**
     * map where key - parent table and values - children tables
     */
    private Map<String, List<PgPartitionTable>> partitionTables;

    private List<String> partitionChildren;

    /**
     * Fills the SQL script with statements based on resolved database actions.
     * Creates a new ActionsToScriptConverter instance and processes all actions in right order.
     *
     * @param script    the SQL script to populate with generated statements
     * @param actions   set of resolved action containers representing database changes
     * @param toRefresh set of statements that need refreshing (for Microsoft SQL modules)
     * @param oldDbFull the complete old database schema for reference
     * @param newDbFull the complete new database schema for reference
     * @param selected  list of user-selected tree elements for filtering actions
     */
    public static void fillScript(SQLScript script,
                                  Set<ActionContainer> actions, Set<IStatement> toRefresh,
                                  IDatabase oldDbFull, IDatabase newDbFull, List<TreeElement> selected) {
        new ActionsToScriptConverter(script, actions, toRefresh, oldDbFull, newDbFull).fillScript(selected);
    }

    /**
     * Creates a new ActionsToScriptConverter with the specified parameters.
     * Initializes internal structures for data movement mode if enabled in settings.
     *
     * @param script    the SQL script to populate with generated statements
     * @param actions   set of resolved action containers representing database changes
     * @param toRefresh ordered set of statements requiring refresh operations (in reverse order)
     * @param oldDbFull the complete old database schema for reference and data movement
     * @param newDbFull the complete new database schema for reference and data movement
     */
    public ActionsToScriptConverter(SQLScript script, Set<ActionContainer> actions,
                                    Set<IStatement> toRefresh, IDatabase oldDbFull, IDatabase newDbFull) {
        this.script = script;
        this.actions = actions;
        this.toRefresh = toRefresh;
        this.oldDbFull = oldDbFull;
        this.newDbFull = newDbFull;
        this.settings = script.getSettings();
        if (settings.isDataMovementMode()) {
            tblTmpNames = new HashMap<>();
            tblIdentityCols = new HashMap<>();
            partitionTables = new HashMap<>();
            partitionChildren = new ArrayList<>();
        }
    }

    /**
     * Fills the script with database objects based on their dependency order.
     *
     * @param selected list of user-selected tree elements for filtering actions
     */
    private void fillScript(List<TreeElement> selected) {
        Set<IStatement> refreshed = new HashSet<>(toRefresh.size());
        if (settings.isDataMovementMode()) {
            fillPartitionTables();
        }

        fillJoinableTableActions();
        for (ActionContainer action : actions) {
            if (toSkip.contains(action)) {
                continue;
            }

            var obj = action.getOldObj();

            if (toRefresh.contains(obj)) {
                if (action.getState() == ObjectState.CREATE && obj instanceof MsView) {
                    // emit refreshes for views only
                    // refreshes for other objects serve as markers
                    // that allow us to skip unmodified drop+create pairs
                    script.addStatement(REFRESH_MODULE.formatted(
                            PgDiffUtils.quoteString(obj.getQualifiedName())));
                    refreshed.add(obj);
                }
            } else if (!hideAction(action, selected)) {
                printAction(action, obj);
            }
        }

        // As a result of discussion with the SQL database developers, it was
        // decided that, in pgCodeKeeper, refresh operations are required only
        // for MsView objects. This is why a filter is used here that only
        // leaves refresh operations for MsView objects.
        //
        // if any refreshes were not emitted as statement replacements
        // add them explicitly in reverse order (the resolver adds them in "drop order")
        IStatement[] orphanRefreshes = toRefresh.stream()
                .filter(r -> r instanceof MsView && !refreshed.contains(r))
                .toArray(IStatement[]::new);
        for (int i = orphanRefreshes.length - 1; i >= 0; --i) {
            script.addStatement(REFRESH_MODULE.formatted(
                    PgDiffUtils.quoteString(orphanRefreshes[i].getQualifiedName())));
        }
    }

    /**
     * Collects joinable table actions that can be joined into single ALTER TABLE statements.
     */
    private void fillJoinableTableActions() {
        List<List<ActionContainer>> changedColumnTables = new ArrayList<>();
        String previousParent = null;
        List<ActionContainer> currentList = null;
        for (ActionContainer action : actions) {
            var oldObj = action.getOldObj();
            if (action.getState() == ObjectState.ALTER && oldObj instanceof PgColumn oldCol
                    && oldCol.isJoinable((PgColumn) action.getNewObj())) {
                String parent = oldObj.getParent().getQualifiedName();
                if (!parent.equals(previousParent)) {
                    currentList = new ArrayList<>();
                    changedColumnTables.add(currentList);
                    previousParent = parent;
                }

                currentList.add(action);
            } else {
                previousParent = null;
            }
        }

        // filling joinableTableActions map where:
        //   key - first action
        //   value - all joinable actions for table
        for (List<ActionContainer> tableChanges : changedColumnTables) {
            if (tableChanges.size() == 1) {
                continue;
            }
            boolean isFirst = true;
            for (ActionContainer action : tableChanges) {
                if (isFirst) {
                    joinableTableActions.put(action, tableChanges);
                    isFirst = false;
                } else {
                    toSkip.add(action);
                }
            }
        }
    }

    private void printAction(ActionContainer action, IStatement obj) {
        String depcy = getComment(action, obj);
        switch (action.getState()) {
            case CREATE:
                if (depcy != null) {
                    script.addStatementWithoutSeparator(depcy);
                }

                var oldObj = obj.getTwin(oldDbFull);

                // explicitly deleting a sequence due to a name conflict
                if (settings.isDataMovementMode() && obj instanceof PgSequence && oldObj != null) {
                    addToDropScript(obj, false);
                }

                if (settings.isDropBeforeCreate() && obj.canDropBeforeCreate()) {
                    addToDropScript(obj, true);
                }

                addToAddScript(obj);

                if (settings.isDataMovementMode() && oldObj instanceof AbstractTable oldTable) {
                    moveData(oldTable, obj);
                }
                break;
            case DROP:
                if (depcy != null) {
                    script.addStatementWithoutSeparator(depcy);
                }
                if (settings.isDataMovementMode()
                        && DbObjType.TABLE == obj.getStatementType()
                        && !(obj instanceof IForeignTable)
                        && obj.getTwin(newDbFull) != null) {
                    addCommandsForRenameTbl((AbstractTable) obj);
                } else {
                    checkMsTableOptions(obj);
                    addToDropScript(obj, false);
                }
                break;
            case ALTER:
                var joinableActions = joinableTableActions.get(action);
                if (joinableActions != null) {
                    getAlterTableScript(joinableActions);
                    return;
                }
                SQLScript temp = new SQLScript(settings);
                ObjectState state = obj.appendAlterSQL(action.getNewObj(), temp);

                if (state.in(ObjectState.ALTER, ObjectState.ALTER_WITH_DEP)) {
                    if (depcy != null) {
                        script.addStatementWithoutSeparator(depcy);
                    }
                    script.addAllStatements(temp);
                }
                break;
            default:
                throw new IllegalStateException("Not implemented action");
        }
    }

    private void checkMsTableOptions(IStatement obj) {
        if (obj instanceof MsConstraintPk && obj.getParent() instanceof MsTable oldTable) {
            MsTable newTable = (MsTable) oldTable.getTwin(newDbFull);
            if (oldTable.compare(newTable)) {
                oldTable.compareTableOptions(newTable, script);
            }
        }
    }

    private void addToAddScript(IStatement obj) {
        obj.getCreationSQL(script);
    }

    private void addToDropScript(IStatement obj, boolean isExist) {
        // check "drop before create"
        if (!droppedObjects.add(obj.getTwin(oldDbFull))) {
            return;
        }
        obj.getDropSQL(script, isExist);
    }

    /**
     * Generates ALTER TABLE script with all joinable changes.
     *
     * @param actionsList list of joinable action containers for the same table
     */
    private void getAlterTableScript(List<ActionContainer> actionsList) {
        StringBuilder sb = new StringBuilder();
        int i = 1;
        for (var colAction : actionsList) {
            PgColumn oldNextCol = (PgColumn) colAction.getOldObj();
            PgColumn newNextCol = (PgColumn) colAction.getNewObj();
            oldNextCol.joinAction(sb, newNextCol, i == 1, i == actionsList.size(), settings);
            i++;
        }
        script.addStatement(sb);
    }

    private void fillPartitionTables() {
        for (ActionContainer action : actions) {
            var obj = action.getOldObj();

            if (action.getState() == ObjectState.CREATE && obj instanceof PgPartitionTable table) {
                partitionTables.computeIfAbsent(table.getParentTable(), tables -> new ArrayList<>()).add(table);
            }
        }

        Iterator<Entry<String, List<PgPartitionTable>>> iterator = partitionTables.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<String, List<PgPartitionTable>> next = iterator.next();
            String parent = next.getKey();
            for (Entry<String, List<PgPartitionTable>> partitions : partitionTables.entrySet()) {
                List<PgPartitionTable> tables = partitions.getValue();
                if (tables.stream().map(AbstractStatement::getQualifiedName).anyMatch(el -> el.equals(parent))) {
                    tables.addAll(next.getValue());
                    iterator.remove();
                    break;
                }
            }
        }

        partitionChildren = partitionTables.values().stream()
                .flatMap(List::stream)
                .map(AbstractStatement::getQualifiedName)
                .toList();
    }

    private void moveData(AbstractTable oldTable, IStatement newObj) {
        String qname = newObj.getQualifiedName();
        String tempName = tblTmpNames.get(qname);
        if (tempName == null) {
            return;
        }

        List<PgPartitionTable> tables = partitionTables.get(qname);
        if (tables != null) {
            // print create for partition tables
            for (PgPartitionTable table : tables) {
                addToAddScript(table);
            }
        }

        oldTable.appendMoveDataSql(newObj, script, tempName, tblIdentityCols.get(qname));

        if (tables != null) {
            List<PgPartitionTable> list = new ArrayList<>(tables);
            Collections.reverse(list);
            list.forEach(this::printDropTempTable);
        }

        printDropTempTable(oldTable);
    }

    private void printDropTempTable(AbstractTable table) {
        String tblTmpName = tblTmpNames.get(table.getQualifiedName());
        if (tblTmpName != null) {
            UnaryOperator<String> quoter = Utils.getQuoter(settings.getDbType());
            StringBuilder sb = new StringBuilder();
            sb.append("DROP TABLE ")
                    .append(quoter.apply(table.getSchemaName())).append('.').append(quoter.apply(tblTmpName));
            script.addStatement(sb);
        }
    }

    private String getComment(ActionContainer action, IStatement oldObj) {
        IStatement objStarter = action.getStarter();
        if (objStarter == null || objStarter == oldObj || objStarter == action.getNewObj()) {
            return null;
        }

        // skip column to parent
        if (objStarter.getStatementType() == DbObjType.COLUMN && objStarter.getParent().equals(oldObj)) {
            return null;
        }

        // skip partition tables in data move mode
        if (settings.isDataMovementMode() && partitionChildren.contains(oldObj.getQualifiedName())) {
            return null;
        }

        return (action.getState() == ObjectState.CREATE ?
                CREATE_COMMENT : DROP_COMMENT).formatted(
                oldObj.getStatementType(),
                oldObj.getBareName(),
                objStarter.getStatementType(),
                objStarter.getQualifiedName());
    }

    /**
     * Determines whether an action should be hidden from the generated script.
     * Checks various conditions including object drop capability, user selection mode,
     * and allowed object types configuration.
     *
     * @param action   the action container to evaluate
     * @param selected list of user-selected tree elements
     * @return true if action should be hidden, false if it may be executed
     */
    private boolean hideAction(ActionContainer action, List<TreeElement> selected) {
        var obj = action.getOldObj();
        if (action.getState() == ObjectState.DROP && !obj.canDrop()) {
            addHiddenObj(action, "object cannot be dropped");
            return true;
        }
        if (settings.isSelectedOnly() && !isSelectedAction(action, selected)) {
            addHiddenObj(action, "cannot change unselected objects in selected-only mode");
            return true;
        }

        DbObjType type = obj.getStatementType();
        if (type == DbObjType.COLUMN) {
            type = DbObjType.TABLE;
        }
        Collection<DbObjType> allowedTypes = settings.getAllowedTypes();
        if (!allowedTypes.isEmpty() && !allowedTypes.contains(type)) {
            if (settings.isStopNotAllowed()) {
                throw new NotAllowedObjectException(action.getOldObj().getQualifiedName()
                        + " (" + type + ") is not an allowed script object. Stopping.");
            }
            addHiddenObj(action, "object type is not in allowed types list");
            return true;
        }

        return false;
    }

    private void addHiddenObj(ActionContainer action, String reason) {
        var old = action.getOldObj();
        String message = HIDDEN_OBJECT.formatted(
                old.getQualifiedName(), old.getStatementType(), action.getState(), reason);
        script.addStatement(message);
    }

    /**
     * Determines whether an action object has been selected in the diff panel.
     *
     * @param action   script action element
     * @param selected collection of selected elements in diff panel
     * @return true if the action object was selected in the diff panel, false otherwise
     */
    private boolean isSelectedAction(ActionContainer action, List<TreeElement> selected) {
        Predicate<IStatement> isSelectedObj = obj ->
                selected.stream()
                        .filter(e -> e.getType().equals(obj.getStatementType()))
                        .filter(e -> e.getName().equals(obj.getName()))
                        .map(e -> e.getStatement(obj.getDatabase()))
                        .anyMatch(obj::equals);

        return switch (action.getState()) {
            case CREATE -> isSelectedObj.test(action.getNewObj());
            case ALTER -> isSelectedObj.test(action.getNewObj()) && isSelectedObj.test(action.getOldObj());
            case DROP -> isSelectedObj.test(action.getOldObj());
            default -> throw new IllegalStateException("Not implemented action");
        };
    }

    /**
     * Adds commands to the script for rename the original table name to a
     * temporary name, given the constraints. Fills the maps {@link #tblTmpNames}
     * and {@link #tblIdentityCols} for use them later (when adding commands to
     * move data from a temporary table to a new table).
     *
     * @param oldTbl the original table to be renamed to a temporary name
     */
    private void addCommandsForRenameTbl(AbstractTable oldTbl) {
        String qname = oldTbl.getQualifiedName();
        String tmpTblName = getTempName(oldTbl);

        script.addStatement(getRenameCommand(oldTbl, tmpTblName));
        tblTmpNames.put(qname, tmpTblName);

        DatabaseType dbtype = settings.getDbType();
        List<String> identityCols = new ArrayList<>();
        for (AbstractColumn col : oldTbl.getColumns()) {
            switch (dbtype) {
                case PG:
                    PgColumn oldPgCol = (PgColumn) col;
                    PgColumn newPgCol = (PgColumn) oldPgCol.getTwin(newDbFull);
                    if (newPgCol != null && newPgCol.getSequence() != null) {
                        AbstractSequence seq = oldPgCol.getSequence();
                        if (seq != null) {
                            script.addStatement(getRenameCommand(seq, getTempName(seq)));
                        }
                        identityCols.add(oldPgCol.getName());
                    }
                    break;
                case MS:
                    MsColumn msCol = (MsColumn) col;
                    if (msCol.isIdentity()) {
                        identityCols.add(msCol.getName());
                    }
                    if (msCol.getDefaultName() != null) {
                        script.addStatement("ALTER TABLE "
                                + MsDiffUtils.quoteName(oldTbl.getSchemaName()) + '.'
                                + MsDiffUtils.quoteName(tmpTblName) + " DROP CONSTRAINT "
                                + MsDiffUtils.quoteName(msCol.getDefaultName()));
                    }
                    break;
                case CH:
                    break;
                default:
                    throw new IllegalArgumentException(Messages.DatabaseType_unsupported_type + dbtype);
            }
        }

        if (!identityCols.isEmpty()) {
            tblIdentityCols.put(qname, identityCols);
        }
    }

    private String getTempName(AbstractStatement st) {
        String tmpSuffix = '_' + UUID.randomUUID().toString().replace("-", "");
        String name = st.getName();
        if (name.length() > 30) {
            return name.substring(0, 30) + tmpSuffix;
        }

        return name + tmpSuffix;
    }

    /**
     * Returns sql command to rename the given object.
     *
     * @param st      object for rename
     * @param newName the new name for given object
     * @return sql command to rename the given object
     */
    private String getRenameCommand(AbstractStatement st, String newName) {
        return switch (settings.getDbType()) {
            case PG -> RENAME_PG_OBJECT.formatted(
                    st.getStatementType(), st.getQualifiedName(), PgDiffUtils.getQuotedName(newName));
            case MS -> RENAME_MS_OBJECT.formatted(
                    PgDiffUtils.quoteString(st.getQualifiedName()), PgDiffUtils.quoteString(newName));
            case CH -> RENAME_CH_OBJECT.formatted(
                    st.getStatementType(), st.getQualifiedName(), ChDiffUtils.getQuotedName(newName));
        };
    }
}
