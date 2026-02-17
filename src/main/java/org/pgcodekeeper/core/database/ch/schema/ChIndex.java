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
import org.pgcodekeeper.core.database.base.schema.SimpleColumn;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;

import java.util.*;

/**
 * Represents a ClickHouse table index.
 * ClickHouse's indexes are used for data skipping and include expression, type, and granularity settings.
 */
public class ChIndex extends ChAbstractStatement implements IIndex {

    private final List<SimpleColumn> columns = new ArrayList<>();
    private final List<String> includes = new ArrayList<>();
    private final Map<String, String> options = new LinkedHashMap<>();

    private String where;
    private String tablespace;
    private boolean unique;
    private boolean isClustered;
    private String expr;
    private String type;
    private int granVal = 1;

    /**
     * Creates a new ClickHouse index with the specified name.
     *
     * @param name the name of the index
     */
    public ChIndex(String name) {
        super(name);
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        script.addStatement(getAlterTable() + " ADD " + getDefinition());
    }

    /**
     * Returns the full definition string for this index.
     *
     * @return the complete index definition
     */
    private String getDefinition() {
        final StringBuilder sb = new StringBuilder();
        sb.append("INDEX ").append(name).append(' ').append(expr)
                .append(" TYPE ").append(type);
        if (granVal != 1) {
            sb.append(" GRANULARITY ").append(granVal);
        }
        return sb.toString();
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        var newIndex = (ChIndex) newCondition;
        if (!compareUnalterable(newIndex)) {
            return ObjectState.RECREATE;
        }
        return ObjectState.NOTHING;
    }

    @Override
    public void getDropSQL(SQLScript script, boolean optionExists) {
        final StringBuilder sb = new StringBuilder();
        sb.append(getAlterTable()).append("\n\tDROP INDEX ");
        if (optionExists) {
            sb.append(IF_EXISTS);
        }
        sb.append(getQuotedName());
        script.addStatement(sb);
    }

    private String getAlterTable() {
        return ((ChTable) parent).getAlterTable(false);
    }

    @Override
    public boolean compareColumns(Collection<String> refs) {
        if (refs.size() != columns.size()) {
            return false;
        }
        int i = 0;
        for (String ref : refs) {
            if (!ref.equals(columns.get(i++).getName())) {
                return false;
            }
        }
        return true;
    }

    public void setClustered(boolean isClustered) {
        this.isClustered = isClustered;
        resetHash();
    }

    public boolean isClustered() {
        return isClustered;
    }

    @Override
    public void addColumn(SimpleColumn column) {
        columns.add(column);
        resetHash();
    }

    @Override
    public void addInclude(String column) {
        includes.add(column);
        resetHash();
    }

    @Override
    public boolean isUnique() {
        return unique;
    }

    public void setUnique(final boolean unique) {
        this.unique = unique;
        resetHash();
    }

    public void setWhere(final String where) {
        this.where = where;
        resetHash();
    }

    public String getTablespace() {
        return tablespace;
    }

    public void setTablespace(String tableSpace) {
        this.tablespace = tableSpace;
        resetHash();
    }

    @Override
    public Map<String, String> getOptions() {
        return Collections.unmodifiableMap(options);
    }

    @Override
    public void addOption(String key, String value) {
        options.put(key, value);
        resetHash();
    }

    public void setExpr(String expr) {
        this.expr = expr;
        resetHash();
    }

    public void setType(String type) {
        this.type = type;
        resetHash();
    }

    public void setGranVal(int granVal) {
        this.granVal = granVal;
        resetHash();
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.put(expr);
        hasher.put(type);
        hasher.put(granVal);
        hasher.putOrdered(columns);
        hasher.put(where);
        hasher.put(includes);
        hasher.put(unique);
        hasher.put(isClustered);
        hasher.put(tablespace);
        hasher.put(options);
    }

    @Override
    public boolean compare(IStatement obj) {
        if (this == obj) {
            return true;
        }
        return obj instanceof ChIndex index
                && super.compare(obj)
                && compareUnalterable(index)
                && isClustered == index.isClustered
                && Objects.equals(tablespace, index.tablespace)
                && Objects.equals(options, index.options);
    }

    protected boolean compareUnalterable(ChIndex index) {
        return Objects.equals(expr, index.expr)
                && Objects.equals(type, index.type)
                && granVal == index.granVal
                && Objects.equals(columns, index.columns)
                && Objects.equals(where, index.where)
                && Objects.equals(includes, index.includes)
                && unique == index.unique;
    }

    @Override
    protected ChIndex getCopy() {
        var index = new ChIndex(name);
        index.setExpr(expr);
        index.setType(type);
        index.setGranVal(granVal);
        index.columns.addAll(columns);
        index.setWhere(where);
        index.includes.addAll(includes);
        index.setUnique(unique);
        index.setClustered(isClustered);
        index.setTablespace(tablespace);
        index.options.putAll(options);
        return index;
    }
}