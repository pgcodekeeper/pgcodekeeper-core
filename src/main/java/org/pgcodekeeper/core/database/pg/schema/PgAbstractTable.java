/*******************************************************************************
 * Copyright 2017-2026 TAXTELECOM, LLC
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
package org.pgcodekeeper.core.database.pg.schema;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.database.base.schema.StatementUtils;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.script.SQLActionType;
import org.pgcodekeeper.core.script.SQLScript;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.utils.Pair;
import org.pgcodekeeper.core.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Base PostgreSQL table class providing common functionality for all PostgreSQL table types.
 * Handles table inheritance, trigger states, column management, and various PostgreSQL-specific
 * table features like WITH OIDS, row-level security, and storage parameters.
 *
 * @author galiev_mr
 * @since 5.3.1.
 */
public abstract class PgAbstractTable extends PgAbstractStatementContainer implements ITable, IOptionContainer {

    protected static final String ALTER_COLUMN = " ALTER COLUMN ";

    /**
     * List of Greenplum-specific storage options.
     */
    private static final List<String> GP_OPTION_LIST = List.of(
            "appendonly",
            "appendoptimized",
            "blocksize",
            "orientation",
            "checksum",
            "compresstype",
            "compresslevel",
            "analyze_hll_non_part_table");

    private static final String RESTART_SEQUENCE_QUERY = """
            DO LANGUAGE plpgsql $_$
            DECLARE restart_var bigint = (SELECT COALESCE(
                (SELECT nextval(pg_get_serial_sequence('%1$s', '%2$s'))),
                (SELECT MAX(%2$s) + 1 FROM %3$s),
                1));
            BEGIN
                EXECUTE $$ ALTER TABLE %3$s ALTER COLUMN %2$s RESTART WITH $$ || restart_var || ';' ;
            END
            $_$""";

    private static final String CHANGE_TRIGGER_STATE =
            "ALTER TABLE %1$s %2$s TRIGGER %3$s";

    protected final List<Inherits> inherits = new ArrayList<>();
    protected final List<PgColumn> columns = new ArrayList<>();
    protected final Map<String, String> options = new LinkedHashMap<>();

    protected boolean hasOids;

    private static final Logger LOG = LoggerFactory.getLogger(PgAbstractTable.class);
    private final Map<String, PgConstraint> constraints = new LinkedHashMap<>();
    private final Map<String, String> triggerStates = new HashMap<>();

    protected PgAbstractTable(String name) {
        super(name);
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        final StringBuilder sbSQL = new StringBuilder();

        SQLScript temp = new SQLScript(script.getSettings(), getSeparator());

        appendName(sbSQL, script.getSettings());
        appendColumns(sbSQL, temp);
        appendInherit(sbSQL);
        appendOptions(sbSQL);
        script.addStatement(sbSQL);

        script.addAllStatements(temp);

        appendNotNullTableConstraints(script);
        appendAlterOptions(script);

        appendOwnerSQL(script);
        appendPrivileges(script);
        appendColumnsPrivileges(script);
        appendColumnsStatistics(script);
        appendTriggerStates(script);
        appendComments(script);
    }

    /**
     * Fills tables parents, parents are stored in 'inherits' list.<br>
     * May be overridden by subclasses.
     * <br><br>
     * For example:
     * <br><br>
     * INHERITS (first_parent, schema_name.second_parent)
     *
     * @param sbSQL - StringBuilder for inherits
     */
    protected void appendInherit(StringBuilder sbSQL) {
        if (!inherits.isEmpty()) {
            sbSQL.append("\nINHERITS (");
            for (final Inherits tableName : inherits) {
                sbSQL.append(tableName.getQualifiedName());
                sbSQL.append(", ");
            }
            sbSQL.setLength(sbSQL.length() - 2);
            sbSQL.append(")");
        }
    }

    private void appendNotNullTableConstraints(SQLScript script) {
        columns.forEach(col -> {
            var notNullConstraint = col.getNotNullConstraint();
            if (notNullConstraint == null ) {
                return;
            }

            if (notNullConstraint.isNotValid()) {
                notNullConstraint.getCreationSQL(script);
            } else if (notNullConstraint.isComplexNotNull()) {
                notNullConstraint.appendOptions(script, new StringBuilder(), false);
            }
        });
    }

    protected void appendColumnsPrivileges(SQLScript script) {
        for (PgColumn col : columns) {
            col.appendPrivileges(script);
        }
    }

    protected void appendColumnsStatistics(SQLScript script) {
        columns.stream().map(PgColumn.class::cast).filter(c -> c.getStatistics() != null)
                .forEach(column -> {
                    StringBuilder sql = new StringBuilder();
                    sql.append(getAlterTable(true));
                    sql.append(ALTER_COLUMN);
                    sql.append(column.getQuotedName());
                    sql.append(" SET STATISTICS ");
                    sql.append(column.getStatistics());
                    script.addStatement(sql);
                });
    }

    @Override
    public void appendComments(SQLScript script) {
        super.appendComments(script);
        appendChildrenComments(script);
    }

    private void appendChildrenComments(SQLScript script) {
        for (var column : columns) {
            column.appendComments(script);
        }
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        int startSize = script.getSize();
        PgAbstractTable newTable = (PgAbstractTable) newCondition;

        if (isRecreated(newTable, script.getSettings())) {
            return ObjectState.RECREATE;
        }

        compareTableTypes(newTable, script);
        compareInherits(newTable, script);
        compareOptions(newTable, script);
        appendAlterOwner(newTable, script);
        compareTableOptions(newTable, script);
        alterPrivileges(newTable, script);
        compareTriggerStates(newTable, script);
        appendAlterComments(newTable, script);

        return getObjectState(script, startSize);
    }

    @Override
    public final boolean isRecreated(ITable newTable, ISettings settings) {
        return newTable instanceof PgAbstractTable newPgTable &&
                (isNeedRecreate(newPgTable) || isColumnsOrderChanged(newPgTable, settings));
    }

    protected boolean isNeedRecreate(PgAbstractTable newTable) {
        if (options.equals(newTable.getOptions())) {
            return false;
        }

        // check greenplum options
        for (String gpOption : GP_OPTION_LIST) {
            if (!Objects.equals(options.get(gpOption), newTable.getOption(gpOption))) {
                return true;
            }
        }

        return false;
    }

    protected boolean isColumnsOrderChanged(PgAbstractTable newTable, ISettings settings) {
        // broken inherit algorithm
        if (!(newTable instanceof PgTypedTable)
                && inherits.isEmpty() && newTable.inherits.isEmpty()) {
            if (settings.isIgnoreColumnOrder()) {
                return false;
            }

            return StatementUtils.isColumnsOrderChanged(newTable.columns, columns);
        }

        return false;
    }

    protected void compareInherits(PgAbstractTable newTable, SQLScript script) {
        List<Inherits> newInherits = newTable.inherits;

        if (newTable instanceof PgPartitionTable) {
            return;
        }

        inherits.stream()
                .filter(e -> !newInherits.contains(e))
                .forEach(e -> script.addStatement(getInheritsActions(e, "\n\tNO INHERIT ")));

        newInherits.stream()
                .filter(e -> !inherits.contains(e))
                .forEach(e -> script.addStatement(getInheritsActions(e, "\n\tINHERIT ")));
    }

    private String getInheritsActions(Inherits inh, String state) {
        return getAlterTable(false) + state + inh.getQualifiedName();
    }

    /**
     * Compare <b>TABLE</b> options by alter table statement
     *
     * @param newTable - new table
     * @param script   - script for statements
     */
    protected void compareTableOptions(PgAbstractTable newTable, SQLScript script) {
        if (hasOids != newTable.hasOids) {
            StringBuilder sql = new StringBuilder();
            sql.append(getAlterTable(true))
                    .append(" SET ")
                    .append(newTable.hasOids ? "WITH" : "WITHOUT")
                    .append(" OIDS");
            script.addStatement(sql);
        }
    }

    private void compareTriggerStates(PgAbstractTable newTable, SQLScript script) {
        var newTriggers = newTable.triggerStates;
        if (!triggerStates.equals(newTriggers)) {
            newTriggers.entrySet().stream()
                    .filter(tr -> !Objects.equals(tr.getValue(), triggerStates.get(tr.getKey())))
                    .forEach(tr -> addTriggerToScript(tr, script));
        }
    }

    private void appendTriggerStates(SQLScript script) {
        for (var state : triggerStates.entrySet()) {
            addTriggerToScript(state, script);
        }
    }

    private void addTriggerToScript(Entry<String, String> tg, SQLScript script) {
        String changeTgState = CHANGE_TRIGGER_STATE.formatted(getQualifiedName(), tg.getValue(), tg.getKey());
        script.addStatement(changeTgState, SQLActionType.END);
    }

    /**
     * Sorts columns on table.
     * <br><br>
     * First the usual columns in the order of adding,
     * then sorted alphabetically the inheritance columns
     */
    public void sortColumns() {
        if (inherits.isEmpty()) {
            return;
        }

        columns.sort((e1, e2) -> {
            boolean first = e1.isInherit();
            boolean second = e2.isInherit();
            if (first && second) {
                return e1.getName().compareTo(e2.getName());
            } else {
                return -Boolean.compare(first, second);
            }
        });

        resetHash();
    }

    @Override
    public boolean compareIgnoringColumnOrder(ITable newTable) {
        return compare(newTable, false);
    }

    @Override
    public void appendMoveDataSql(IStatement newCondition, SQLScript script, String tblTmpBareName,
                                  List<String> identityCols) {
        PgAbstractTable newTable = (PgAbstractTable) newCondition;
        List<String> colsForMovingData = getColsForMovingData(newTable);
        if (colsForMovingData.isEmpty()) {
            return;
        }

        String tblTmpQName = getParent().getQuotedName() + '.' + quote(tblTmpBareName);
        String cols = colsForMovingData.stream().map(this::quote).collect(Collectors.joining(", "));
        List<String> identityColsForMovingData = identityCols == null ? Collections.emptyList()
                : identityCols.stream().filter(colsForMovingData::contains).toList();
        writeInsert(script, newTable, tblTmpQName, identityColsForMovingData, cols);
    }

    /**
     * Returns the names of the columns from which data will be moved to another table, excluding calculated columns.
     */
    private List<String> getColsForMovingData(PgAbstractTable newTable) {
        return newTable.getColumns().stream()
                .filter(c -> getColumn(c.getName()) != null)
                .map(PgColumn.class::cast)
                .filter(pgCol -> !pgCol.isGenerated())
                .map(PgColumn::getName)
                .toList();
    }

    private void writeInsert(SQLScript script, PgAbstractTable newTable, String tblTmpQName,
                               List<String> identityColsForMovingData, String cols) {
        String tblQName = newTable.getQualifiedName();
        StringBuilder sbInsert = new StringBuilder();
        sbInsert.append("INSERT INTO ").append(tblQName).append('(').append(cols).append(")");
        if (!identityColsForMovingData.isEmpty()) {
            sbInsert.append("\nOVERRIDING SYSTEM VALUE");
        }
        sbInsert.append("\nSELECT ").append(cols).append(" FROM ").append(tblTmpQName);
        script.addStatement(sbInsert);

        for (String colName : identityColsForMovingData) {
            script.addStatement(RESTART_SEQUENCE_QUERY.formatted(tblTmpQName, quote(colName), tblQName));
        }
    }

    protected void writeColumn(PgColumn column, StringBuilder sbSQL, SQLScript script) {
        boolean isInherit = column.isInherit();
        if (isInherit) {
            fillInheritOptions(column, script);
        } else {
            sbSQL.append("\t");
            sbSQL.append(column.getFullDefinition());
            sbSQL.append(",\n");
        }
        if (column.getStorage() != null) {
            StringBuilder sql = new StringBuilder();
            sql.append(getAlterTable(isInherit))
                    .append(ALTER_COLUMN)
                    .append(column.getQuotedName())
                    .append(" SET STORAGE ")
                    .append(column.getStorage());
            script.addStatement(sql);
        }

        writeOptions(column, script, isInherit);
        PgSequence sequence = column.getSequence();
        if (sequence != null) {
            StringBuilder sbSeq = new StringBuilder();
            if (script.getSettings().isGenerateExistDoBlock()) {
                StringBuilder tmpSb = new StringBuilder();
                writeSequences(column, tmpSb);
                appendSqlWrappedInDo(sbSeq, tmpSb, DUPLICATE_RELATION);
            } else {
                writeSequences(column, sbSeq);
                sbSeq.setLength(sbSeq.length() - 1);
            }
            script.addStatement(sbSeq);
        }
    }

    private void fillInheritOptions(PgColumn column, SQLScript script) {
        if (column.isNotNull()) {
            script.addStatement(getAlterColumn(column) + " SET NOT NULL");
        }
        if (column.getDefaultValue() != null) {
            script.addStatement(getAlterColumn(column) + " SET DEFAULT " + column.getDefaultValue());
        }
    }

    private String getAlterColumn(PgColumn column) {
        return getAlterTable(true) + ALTER_COLUMN + column.getQuotedName();
    }

    private void writeOptions(PgColumn column, SQLScript script, boolean isInherit) {
        Map<String, String> opts = column.getOptions();
        Map<String, String> fOpts = column.getForeignOptions();

        if (!opts.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append(getAlterTable(isInherit))
                    .append(ALTER_COLUMN)
                    .append(column.getQuotedName())
                    .append(" SET (");

            for (Entry<String, String> option : opts.entrySet()) {
                sb.append(option.getKey());
                if (!option.getValue().isEmpty()) {
                    sb.append('=').append(option.getValue());
                }
                sb.append(", ");
            }
            sb.setLength(sb.length() - 2);
            sb.append(")");
            script.addStatement(sb);
        }

        if (!fOpts.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append(getAlterTable(isInherit))
                    .append(ALTER_COLUMN)
                    .append(column.getQuotedName())
                    .append(" OPTIONS (");

            for (Entry<String, String> option : fOpts.entrySet()) {
                sb.append(option.getKey());
                if (!option.getValue().isEmpty()) {
                    sb.append(' ').append(option.getValue());
                }
                sb.append(", ");
            }
            sb.setLength(sb.length() - 2);
            sb.append(")");
            script.addStatement(sb);
        }
    }

    protected PgSequence writeSequences(PgColumn column, StringBuilder sbOption) {
        PgSequence sequence = column.getSequence();
        sbOption.append(getAlterTable(false))
                .append(ALTER_COLUMN)
                .append(column.getQuotedName())
                .append(" ADD GENERATED ")
                .append(column.getIdentityType())
                .append(" AS IDENTITY (");
        sbOption.append("\n\tSEQUENCE NAME ").append(sequence.getQualifiedName());
        sequence.fillSequenceBody(sbOption);
        sbOption.append("\n);");
        return sequence;
    }

    /**
     * Appends CREATE TABLE statement beginning
     * <br><br>
     * Expected:
     * <br><br>
     * CREATE [ [ GLOBAL | LOCAL ] { TEMPORARY | TEMP } | UNLOGGED | FOREIGN ] TABLE [ IF NOT EXISTS ] table_name
     *
     * @param sbSQL    - StringBuilder for statement
     * @param settings - {@link ISettings} stores settings for correct script generation
     */
    protected abstract void appendName(StringBuilder sbSQL, ISettings settings);

    /**
     * Fills columns and their options to create table statement. Options will be
     * appends after CREATE TABLE statement. <br>
     * Must be overridden by subclasses
     *
     * @param sbSQL  - StringBuilder for columns
     * @param script - collection for options
     */
    protected abstract void appendColumns(StringBuilder sbSQL, SQLScript script);

    /**
     * Appends table storage parameters or server options, part of create statement;
     *
     * @param sbSQL - StringBuilder for options
     */
    protected abstract void appendOptions(StringBuilder sbSQL);

    /**
     * Appends <b>TABLE</b> options by alter table statement
     * <br><br>
     * For example:
     * <br><br>
     * ALTER TABLE table_name SET WITH OID;
     * <br>
     *
     * @param script - SQLScript for options
     */
    protected abstract void appendAlterOptions(SQLScript script);

    /**
     * Compare tables types and generate transform scripts for change tables type
     *
     * @param newTable - new table
     * @param script   - script for statements
     */
    protected abstract void compareTableTypes(PgAbstractTable newTable, SQLScript script);

    /**
     * Generates beginning of alter table statement.
     *
     * @param only if true, append 'ONLY' to statement
     * @return alter table statement beginning in String format
     */
    protected abstract String getAlterTable(boolean only);

    @Override
    public List<IColumn> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    @Override
    public void fillChildrenList(List<Collection<? extends AbstractStatement>> l) {
        super.fillChildrenList(l);
        l.add(constraints.values());
    }

    @Override
    public void addChild(IStatement st) {
        if (DbObjType.CONSTRAINT == st.getStatementType()) {
            addConstraint((PgConstraint) st);
            return;
        }

        super.addChild(st);
    }

    @Override
    public AbstractStatement getChild(String name, DbObjType type) {
        if (DbObjType.CONSTRAINT == type) {
            return getConstraint(name);
        }

        return super.getChild(name, type);
    }

    @Override
    public Collection<IStatement> getChildrenByType(DbObjType type) {
        if (DbObjType.CONSTRAINT == type) {
            return Collections.unmodifiableCollection(constraints.values());
        }
        return super.getChildrenByType(type);
    }

    @Override
    public boolean isClustered() {
        if (super.isClustered()) {
            return true;
        }

        for (PgConstraint constr : constraints.values()) {
            if (constr instanceof IConstraintPk pk && pk.isClustered()) {
                return true;
            }
        }

        return false;
    }

    /**
     * Getter for {@link #constraints}. The list cannot be modified.
     *
     * @return {@link #constraints}
     */
    @Override
    public Collection<PgConstraint> getConstraints() {
        return Collections.unmodifiableCollection(constraints.values());
    }

    @Override
    public void addOption(String option, String value) {
        options.put(option, value);
        resetHash();
    }

    private String getOption(String option) {
        return options.get(option);
    }

    @Override
    public Map<String, String> getOptions() {
        return Collections.unmodifiableMap(options);
    }


    @Override
    public Stream<Pair<String, String>> getRelationColumns() {
        Stream<Pair<String, String>> localColumns = columns.stream()
                .filter(c -> c.getType() != null)
                .map(c -> new Pair<>(c.getName(), c.getType()));
        if (inherits.isEmpty()) {
            return localColumns;
        }

        Stream<Pair<String, String>> inhColumns = Stream.empty();
        for (Inherits inht : inherits) {
            String schemaName = inht.key();
            ISchema inhtSchema = schemaName == null ? getContainingSchema() : getDatabase().getSchema(schemaName);
            if (inhtSchema != null) {
                String tableName = inht.value();
                PgAbstractTable inhtTable = (PgAbstractTable) inhtSchema.getChild(tableName, DbObjType.TABLE);
                if (inhtTable != null) {
                    inhColumns = Stream.concat(inhColumns, inhtTable.getRelationColumns());
                } else {
                    var msg = Messages.AbstractPgTable_log_inherits_not_found.formatted(schemaName, tableName);
                    LOG.warn(msg);
                }
            } else {
                var msg = Messages.AbstractPgTable_log_schemas_not_found.formatted(schemaName);
                LOG.warn(msg);
            }
        }
        return Stream.concat(inhColumns, localColumns);
    }

    /**
     * Adds a parent table to the inheritance list.
     *
     * @param schemaName parent table schema name.
     * @param tableName  parent table name
     */
    public void addInherits(final String schemaName, final String tableName) {
        inherits.add(new Inherits(schemaName, tableName));
        resetHash();
    }

    /**
     * Getter for {@link #inherits}.
     *
     * @return {@link #inherits}
     */
    public List<Inherits> getInherits() {
        return Collections.unmodifiableList(inherits);
    }

    /**
     * Checks if this table has any inheritance relationships.
     *
     * @return true if table inherits from other tables
     */
    public boolean hasInherits() {
        return !inherits.isEmpty();
    }

    /**
     * Sets the state of a specific trigger on this table.
     *
     * @param triggerName name of the trigger
     * @param state       desired trigger state
     */
    public void putTriggerState(String triggerName, PgTriggerState state) {
        triggerStates.put(triggerName, state.getValue());
    }

    public void setHasOids(final boolean hasOids) {
        this.hasOids = hasOids;
        resetHash();
    }

    public PgConstraint getConstraint(final String name) {
        var constraint = getChildByName(constraints, name);
        if (constraint != null) {
            return constraint;
        }

        return columns.stream()
                .map(PgColumn::getNotNullConstraint)
                .filter(Objects::nonNull)
                .filter(notNullConstraint -> notNullConstraint.getName().equals(name))
                .findAny()
                .orElse(null);
    }

    /**
     * Finds column according to specified column {@code name}.
     *
     * @param name name of the column to be searched
     * @return found column or null if no such column has been found
     */
    @Override
    public PgColumn getColumn(final String name) {
        for (PgColumn column : columns) {
            if (column.getName().equals(name)) {
                return column;
            }
        }
        return null;
    }

    protected void addConstraint(PgConstraint constraint) {
        addUnique(constraints, constraint);
    }

    public void addColumn(final PgColumn column) {
        assertUnique(getColumn(column.getName()), column);
        columns.add(column);
        column.setParent(this);
        resetHash();
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.putOrdered(columns);
        hasher.put(options);
        hasher.put(hasOids);
        hasher.putOrdered(inherits);
        hasher.put(triggerStates);
    }

    @Override
    public void computeChildrenHash(Hasher hasher) {
        super.computeChildrenHash(hasher);
        hasher.putUnordered(constraints);
    }

    @Override
    public boolean compare(IStatement obj) {
        return compare(obj, true);
    }

    private boolean compare(IStatement obj, boolean checkColumnOrder) {
        if (obj instanceof PgAbstractTable table && super.compare(obj)) {
            boolean isColumnsEqual;
            if (checkColumnOrder) {
                isColumnsEqual = columns.equals(table.columns);
            } else {
                isColumnsEqual = Utils.setLikeEquals(columns, table.columns);
            }

            return isColumnsEqual
                    && getClass().equals(table.getClass())
                    && options.equals(table.options)
                    && compareTable(table);
        }

        return false;
    }

    protected boolean compareTable(PgAbstractTable table) {
        return hasOids == table.hasOids
                && inherits.equals(table.inherits)
                && triggerStates.equals(table.triggerStates);
    }

    @Override
    public boolean compareChildren(AbstractStatement obj) {
        return obj instanceof PgAbstractTable table && super.compareChildren(obj)
                && constraints.equals(table.constraints);
    }

    @Override
    protected PgAbstractTable getCopy() {
        PgAbstractTable copy = getTableCopy();
        for (PgColumn colSrc : columns) {
            copy.addColumn((PgColumn) colSrc.deepCopy());
        }
        copy.options.putAll(options);
        copy.setHasOids(hasOids);
        copy.inherits.addAll(inherits);
        copy.triggerStates.putAll(triggerStates);
        return copy;
    }

    protected abstract PgAbstractTable getTableCopy();
}
