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
package org.pgcodekeeper.core.database.base.schema;

import org.pgcodekeeper.core.Utils;
import org.pgcodekeeper.core.database.api.schema.IOptionContainer;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.script.SQLScript;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.utils.Pair;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Abstract base class for database tables.
 * Contains columns, constraints, indexes, triggers, and other table-related objects.
 * Provides common functionality for tables across different database types.
 */
public abstract class AbstractTable extends AbstractStatementContainer implements IOptionContainer {

    protected static final String ALTER_COLUMN = " ALTER COLUMN ";

    protected final List<AbstractColumn> columns = new ArrayList<>();
    protected final Map<String, String> options = new LinkedHashMap<>();

    private final Map<String, AbstractConstraint> constraints = new LinkedHashMap<>();

    @Override
    public DbObjType getStatementType() {
        return DbObjType.TABLE;
    }

    protected AbstractTable(String name) {
        super(name);
    }

    @Override
    protected void fillChildrenList(List<Collection<? extends AbstractStatement>> l) {
        super.fillChildrenList(l);
        l.add(constraints.values());
    }

    /**
     * Creates a stream that includes the statement itself and its columns if it's a table.
     *
     * @param st the statement to process
     * @return a stream containing the statement and its columns (if applicable)
     */
    public static Stream<? extends IStatement> columnAdder(IStatement st) {
        Stream<IStatement> newStream = Stream.of(st);
        if (st instanceof AbstractTable table) {
            newStream = Stream.concat(newStream, table.columns.stream());
        }

        return newStream;
    }

    /**
     * Generates beginning of alter table statement.
     *
     * @param only if true, append 'ONLY' to statement
     * @return alter table statement beginning in String format
     */
    public abstract String getAlterTable(boolean only);

    /**
     * Finds column according to specified column {@code name}.
     *
     * @param name name of the column to be searched
     * @return found column or null if no such column has been found
     */
    public AbstractColumn getColumn(final String name) {
        for (AbstractColumn column : columns) {
            if (column.name.equals(name)) {
                return column;
            }
        }
        return null;
    }

    /**
     * Getter for {@link #columns}. The list cannot be modified.
     *
     * @return {@link #columns}
     */
    public List<AbstractColumn> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    @Override
    public Stream<Pair<String, String>> getRelationColumns() {
        return columns.stream()
                .filter(c -> c.getType() != null)
                .map(c -> new Pair<>(c.getName(), c.getType()));
    }

    @Override
    public AbstractConstraint getConstraint(final String name) {
        return getChildByName(constraints, name);
    }

    /**
     * Getter for {@link #constraints}. The list cannot be modified.
     *
     * @return {@link #constraints}
     */
    @Override
    public Collection<AbstractConstraint> getConstraints() {
        return Collections.unmodifiableCollection(constraints.values());
    }

    protected boolean isColumnsOrderChanged(AbstractTable newTable, ISettings settings) {
        if (settings.isIgnoreColumnOrder()) {
            return false;
        }

        return StatementUtils.isColumnsOrderChanged(newTable.columns, columns);
    }

    protected void appendColumnsPriliges(SQLScript script) {
        for (AbstractColumn col : columns) {
            col.appendPrivileges(script);
        }
    }

    /**
     * Compares this table with the {@code newTable} to determine if a full table recreation is required.
     * A full recreation (DROP and CREATE) is needed when the tables differ in ways that cannot
     * be altered using ALTER TABLE statements.
     *
     * @param newTable the new table definition to compare against
     * @param settings application settings that may affect the comparison logic
     * @return {@code true} if the table requires recreation (DROP and CREATE) rather than
     * being alterable, {@code false} if the changes can be applied via ALTER TABLE
     */
    public final boolean isRecreated(AbstractTable newTable, ISettings settings) {
        return isNeedRecreate(newTable) || isColumnsOrderChanged(newTable, settings);
    }

    protected abstract boolean isNeedRecreate(AbstractTable newTable);

    @Override
    public Map<String, String> getOptions() {
        return Collections.unmodifiableMap(options);
    }

    /**
     * Gets the value for the specified option.
     *
     * @param option the option key
     * @return the option value, or null if not found
     */
    public String getOption(String option) {
        return options.get(option);
    }

    @Override
    public void addOption(String option, String value) {
        options.put(option, value);
        resetHash();
    }

    /**
     * Adds a column to the table.
     *
     * @param column the column to add
     */
    public void addColumn(final AbstractColumn column) {
        assertUnique(getColumn(column.getName()), column);
        columns.add(column);
        column.setParent(this);
        resetHash();
    }

    @Override
    public void addConstraint(final AbstractConstraint constraint) {
        addUnique(constraints, constraint);
    }

    /**
     * Checks if a column with the specified name exists.
     *
     * @param name the column name
     * @return true if column exists, false otherwise
     */
    public boolean containsColumn(final String name) {
        return getColumn(name) != null;
    }

    @Override
    public boolean compare(IStatement obj) {
        return compare(obj, true);
    }

    protected abstract boolean compareTable(AbstractStatement obj);

    private boolean compare(IStatement obj, boolean checkColumnOrder) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof AbstractTable table && super.compare(obj)) {
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

    /**
     * Checks that tables are equal regardless of column order.
     *
     * @param oldTable old state of the table
     * @param newTable new state of the table
     * @return true if the tables are identical
     */
    public static boolean compareIgnoringColumnOrder(AbstractTable oldTable, AbstractTable newTable) {
        return oldTable.compare(newTable, false);
    }

    @Override
    public boolean compareChildren(AbstractStatement obj) {
        if (obj instanceof AbstractTable table && super.compareChildren(obj)) {
            return constraints.equals(table.constraints);
        }
        return false;
    }

    @Override
    public void computeChildrenHash(Hasher hasher) {
        super.computeChildrenHash(hasher);
        hasher.putUnordered(constraints);
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.putOrdered(columns);
        hasher.put(options);
    }

    @Override
    public AbstractTable shallowCopy() {
        AbstractTable tableDst = (AbstractTable) super.shallowCopy();
        copyBaseFields(tableDst);
        for (AbstractColumn colSrc : columns) {
            tableDst.addColumn((AbstractColumn) colSrc.deepCopy());
        }
        tableDst.options.putAll(options);
        return tableDst;
    }

    @Override
    protected AbstractStatementContainer getCopy() {
        return getTableCopy();
    }

    protected abstract AbstractTable getTableCopy();

    /**
     * Adds commands to the script for move data from the temporary table to the new table, given the identity columns,
     * and a command to delete the temporary table.
     */
    public void appendMoveDataSql(IStatement newCondition, SQLScript script, String tblTmpBareName,
                                  List<String> identityCols) {
        AbstractTable newTable = (AbstractTable) newCondition;
        List<String> colsForMovingData = getColsForMovingData(newTable);
        if (colsForMovingData.isEmpty()) {
            return;
        }

        var quoter = Utils.getQuoter(getDbType());
        String tblTmpQName = quoter.apply(getSchemaName()) + '.' + quoter.apply(tblTmpBareName);
        String cols = colsForMovingData.stream().map(quoter).collect(Collectors.joining(", "));
        List<String> identityColsForMovingData = identityCols == null ? Collections.emptyList()
                : identityCols.stream().filter(colsForMovingData::contains).toList();
        writeInsert(script, newTable, tblTmpQName, identityColsForMovingData, cols);
    }

    protected abstract void writeInsert(SQLScript script, AbstractTable newTable, String tblTmpQName,
                                        List<String> identityColsForMovingData, String cols);

    /**
     * Returns the names of the columns from which data will be moved to another table, excluding calculated columns.
     */
    protected abstract List<String> getColsForMovingData(AbstractTable newTable);
}
