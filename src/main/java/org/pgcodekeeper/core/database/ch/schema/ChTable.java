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
package org.pgcodekeeper.core.database.ch.schema;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.database.base.schema.StatementUtils;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.utils.Pair;
import org.pgcodekeeper.core.utils.Utils;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Represents a ClickHouse table with engine configuration and projections.
 * Supports ClickHouse-specific features like table engines, projections, and specialized DDL operations.
 */
public class ChTable extends ChAbstractStatement implements ITable, IOptionContainer {

    protected static final String ALTER_COLUMN = " ALTER COLUMN ";
    private final Map<String, ChIndex> indexes = new LinkedHashMap<>();
    private final Map<String, ChConstraint> constraints = new LinkedHashMap<>();

    protected ChEngine engine;
    protected final Map<String, String> projections = new LinkedHashMap<>();
    protected final Map<String, String> options = new LinkedHashMap<>();
    protected final List<ChColumn> columns = new ArrayList<>();

    /**
     * Creates a new ClickHouse table with the specified name.
     *
     * @param name the name of the table
     */
    public ChTable(String name) {
        super(name);
    }

    /**
     * Adds a projection to this table.
     *
     * @param key        the projection name
     * @param expression the projection expression
     */
    public void addProjection(String key, String expression) {
        projections.put(key, expression);
        resetHash();
    }

    public void setEngine(ChEngine engine) {
        this.engine = engine;
        resetHash();
    }

    public void setPkExpr(String pkExpr) {
        engine.setPrimaryKey(pkExpr);
        resetHash();
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        var sb = new StringBuilder();
        sb.append("CREATE TABLE ");
        var settings = script.getSettings();
        appendIfNotExists(sb, settings);
        sb.append(getQualifiedName());
        var clusterName = settings.getClusterName();
        if (null != clusterName && !clusterName.isBlank()) {
            sb.append(" ON CLUSTER ").append(clusterName);
        }
        sb.append("\n(");
        appendTableBody(sb);
        if (isNotEmptyTable()) {
            sb.setLength(sb.length() - 1);
        }
        sb.append("\n)");

        engine.appendCreationSQL(sb);

        if (getComment() != null) {
            sb.append("\nCOMMENT ").append(getComment());
        }
        script.addStatement(sb);
    }

    protected void appendTableBody(StringBuilder sb) {
        for (ChColumn column : columns) {
            sb.append("\n\t").append(column.getFullDefinition()).append(',');
        }

        for (Entry<String, String> proj : projections.entrySet()) {
            sb.append("\n\tPROJECTION ").append(proj.getKey()).append(' ').append(proj.getValue()).append(',');
        }
    }

    protected boolean isNotEmptyTable() {
        return !columns.isEmpty() || !projections.isEmpty();
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        int startSize = script.getSize();
        ChTable newTable = (ChTable) newCondition;

        if (isRecreated(newTable, script.getSettings())) {
            return ObjectState.RECREATE;
        }

        compareProjections(newTable.projections, script);
        engine.appendAlterSQL(newTable.engine, getAlterTable(false), script);
        compareComment(newTable.getComment(), script);
        return getObjectState(script, startSize);
    }

    private void compareProjections(Map<String, String> newProjections, SQLScript script) {
        if (Objects.equals(projections, newProjections)) {
            return;
        }
        Set<String> toDrops = new HashSet<>();
        Map<String, String> toAdds = new LinkedHashMap<>();

        for (String oldKey : projections.keySet()) {
            if (!newProjections.containsKey(oldKey)) {
                toDrops.add(oldKey);
                continue;
            }
            var newValue = newProjections.get(oldKey);
            if (!Objects.equals(newValue, projections.get(oldKey))) {
                toDrops.add(oldKey);
                toAdds.put(oldKey, newValue);
            }
        }

        for (String newKey : newProjections.keySet()) {
            if (!projections.containsKey(newKey)) {
                toAdds.put(newKey, newProjections.get(newKey));
            }
        }

        appendAlterProjections(toDrops, toAdds, script);
    }

    private void appendAlterProjections(Set<String> toDrops, Map<String, String> toAdds, SQLScript script) {
        for (String toDrop : toDrops) {
            script.addStatement(getAlterTable(false) + "\n\tDROP PROJECTION IF EXISTS " + toDrop);
        }
        for (Entry<String, String> toAdd : toAdds.entrySet()) {
            StringBuilder sb = new StringBuilder();
            sb.append(getAlterTable(false)).append("\n\tADD PROJECTION ");
            appendIfNotExists(sb, script.getSettings());
            sb.append(toAdd.getKey()).append(' ').append(toAdd.getValue());
            script.addStatement(sb);
        }
    }

    private void compareComment(String newComment, SQLScript script) {
        if (Objects.equals(getComment(), newComment)) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(getAlterTable(false)).append("\n\tMODIFY COMMENT ");
        sb.append(Objects.requireNonNullElse(newComment, "''"));
        script.addStatement(sb);
    }

    public String getAlterTable(boolean only) {
        return ALTER_TABLE + getQualifiedName();
    }

    protected boolean isNeedRecreate(ChTable newTable) {
        var newEngine = newTable.engine;
        return !engine.compareUnalterable(newEngine)
                && !engine.isModifybleSampleBy(newEngine);
    }

    @Override
    public void compareOptions(IOptionContainer newContainer, SQLScript script) {
        // no impl
    }

    protected void writeInsert(SQLScript script, ChTable newTable, String tblTmpQName,
                               List<String> identityColsForMovingData, String cols) {
        StringBuilder sbInsert = new StringBuilder();
        sbInsert.append("INSERT INTO ").append(newTable.getQualifiedName()).append('(').append(cols).append(")");
        sbInsert.append("\nSELECT ").append(cols).append(" FROM ").append(tblTmpQName);
        script.addStatement(sbInsert);
    }

    protected List<String> getColsForMovingData(ChTable newTable) {
        return newTable.columns.stream()
                .map(IColumn::getName)
                .filter(this::containsColumn)
                .toList();
    }

    @Override
    public DbObjType getStatementType() {
        return DbObjType.TABLE;
    }

    @Override
    public void fillChildrenList(List<Collection<? extends AbstractStatement>> l) {
        l.add(indexes.values());
        l.add(constraints.values());
    }

    /**
     * Checks if this container has any clustered indexes or constraints.
     */
    public boolean isClustered() {
        for (var ind : indexes.values()) {
            if (ind.isClustered()) {
                return true;
            }
        }

        return false;
    }

    @Override
    public AbstractStatement getChild(String name, DbObjType type) {
        return switch (type) {
            case INDEX -> getChildByName(indexes, name);
            case CONSTRAINT -> getChildByName(constraints, name);
            default -> null;
        };
    }

    @Override
    public Collection<IStatement> getChildrenByType(DbObjType type) {
        return switch (type) {
            case INDEX -> Collections.unmodifiableCollection(indexes.values());
            case CONSTRAINT -> Collections.unmodifiableCollection(constraints.values());
            default -> List.of();
        };
    }

    @Override
    public void addChild(IStatement st) {
        DbObjType type = st.getStatementType();
        switch (type) {
            case INDEX:
                addUnique(indexes, (ChIndex) st);
                break;
            case CONSTRAINT:
                addUnique(constraints, (ChConstraint) st);
                break;
            default:
                throw new IllegalArgumentException("Unsupported child type: " + type);
        }
    }

    /**
     * Finds column according to specified column {@code name}.
     *
     * @param name name of the column to be searched
     * @return found column or null if no such column has been found
     */
    @Override
    public ChColumn getColumn(final String name) {
        for (ChColumn column : columns) {
            if (column.getName().equals(name)) {
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
    @Override
    public List<IColumn> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    @Override
    public Stream<Pair<String, String>> getRelationColumns() {
        return columns.stream()
                .filter(c -> c.getType() != null)
                .map(c -> new Pair<>(c.getName(), c.getType()));
    }

    protected boolean isColumnsOrderChanged(ChTable newTable, ISettings settings) {
        if (settings.isIgnoreColumnOrder()) {
            return false;
        }

        return StatementUtils.isColumnsOrderChanged(newTable.columns, columns);
    }

    protected void appendColumnsPrivileges(SQLScript script) {
        for (var col : columns) {
            col.appendPrivileges(script);
        }
    }

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
    public void addColumn(final ChColumn column) {
        assertUnique(getColumn(column.getName()), column);
        columns.add(column);
        column.setParent(this);
        resetHash();
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
    public void computeChildrenHash(Hasher hasher) {
        hasher.putUnordered(constraints);
        hasher.putUnordered(indexes);
    }

    @Override
    public boolean compareChildren(AbstractStatement obj) {
        if (obj instanceof ChTable table && super.compareChildren(obj)) {
            return constraints.equals(table.constraints)
                    && indexes.equals(table.indexes);
        }
        return false;
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.putOrdered(columns);
        hasher.put(options);
        hasher.put(projections);
        hasher.put(engine);
    }

    @Override
    public boolean compare(IStatement obj) {
        return compare(obj, true);
    }

    private boolean compare(IStatement obj, boolean checkColumnOrder) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof ChTable table && super.compare(obj)) {
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

    protected boolean compareTable(AbstractStatement obj) {
        return obj instanceof ChTable table
                && Objects.equals(projections, table.projections)
                && Objects.equals(engine, table.engine);
    }

    @Override
    protected ChTable getCopy() {
        ChTable copy = getTableCopy();
        for (var colSrc : columns) {
            copy.addColumn((ChColumn) colSrc.deepCopy());
        }
        copy.options.putAll(options);
        copy.projections.putAll(projections);
        copy.setEngine(engine);
        return copy;
    }

    protected ChTable getTableCopy() {
        return new ChTable(name);
    }

    @Override
    public void appendMoveDataSql(IStatement newCondition, SQLScript script, String tblTmpBareName,
                                  List<String> identityCols) {
        ChTable newTable = (ChTable) newCondition;
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

    @Override
    public boolean isRecreated(ITable newTable, ISettings settings) {
        return newTable instanceof ChTable newChTable
                && (isNeedRecreate(newChTable) || isColumnsOrderChanged(newChTable, settings));
    }

    @Override
    public boolean compareIgnoringColumnOrder(ITable newTable) {
        return compare(newTable, false);
    }
}
