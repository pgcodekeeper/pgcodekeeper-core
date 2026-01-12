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

import org.pgcodekeeper.core.database.api.schema.IOptionContainer;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.database.base.schema.AbstractColumn;
import org.pgcodekeeper.core.database.base.schema.AbstractTable;
import org.pgcodekeeper.core.database.api.schema.ObjectState;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;

import java.util.*;
import java.util.Map.Entry;

/**
 * Represents a ClickHouse table with engine configuration and projections.
 * Supports ClickHouse-specific features like table engines, projections, and specialized DDL operations.
 */
public class ChTable extends AbstractTable implements IChStatement {

    protected final Map<String, String> projections = new LinkedHashMap<>();

    protected ChEngine engine;

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
        appendIfNotExists(sb, script.getSettings());
        sb.append(getQualifiedName()).append("\n(");
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
        for (AbstractColumn column : columns) {
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

    @Override
    public String getAlterTable(boolean only) {
        return ALTER_TABLE + getQualifiedName();
    }

    @Override
    protected boolean isNeedRecreate(AbstractTable newTable) {
        var newEngine = ((ChTable) newTable).engine;
        return !engine.compareUnalterable(newEngine)
                && !engine.isModifybleSampleBy(newEngine);
    }

    @Override
    public void computeHash(Hasher hasher) {
        super.computeHash(hasher);
        hasher.put(projections);
        hasher.put(engine);
    }

    @Override
    protected boolean compareTable(AbstractStatement obj) {
        return obj instanceof ChTable table
                && Objects.equals(projections, table.projections)
                && Objects.equals(engine, table.engine);
    }

    @Override
    protected AbstractTable getTableCopy() {
        var table = new ChTable(name);
        table.projections.putAll(projections);
        table.setEngine(engine);
        return table;
    }

    @Override
    public void appendComments(SQLScript script) {
        // no impl
    }

    @Override
    protected void appendCommentSql(SQLScript script) {
        // no impl
    }

    @Override
    public void compareOptions(IOptionContainer newContainer, SQLScript script) {
        // no impl
    }

    @Override
    protected void writeInsert(SQLScript script, AbstractTable newTable, String tblTmpQName,
                               List<String> identityColsForMovingData, String cols) {
        StringBuilder sbInsert = new StringBuilder();
        sbInsert.append("INSERT INTO ").append(newTable.getQualifiedName()).append('(').append(cols).append(")");
        sbInsert.append("\nSELECT ").append(cols).append(" FROM ").append(tblTmpQName);
        script.addStatement(sbInsert);
    }

    @Override
    protected List<String> getColsForMovingData(AbstractTable newTable) {
        return newTable.getColumns().stream()
                .map(AbstractColumn::getName)
                .filter(this::containsColumn)
                .toList();
    }
}
