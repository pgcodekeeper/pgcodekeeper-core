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
package org.pgcodekeeper.core.database.ms.schema;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.MsDiffUtils;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;

import java.util.*;

/**
 * Represents Microsoft SQL table statistics.
 * Statistics are used by the query optimizer to create efficient query execution plans.
 */
public final class MsStatistics extends AbstractStatistics implements ISubElement {

    public static final String SAMPLE = "SAMPLE";
    private String filter;
    private String samplePercent;
    private final List<String> cols = new ArrayList<>();
    private final Map<String, String> options = new HashMap<>();
    private boolean isParentHasData;

    /**
     * Creates a new Microsoft SQL statistics object.
     *
     * @param name the statistics name
     */
    public MsStatistics(String name) {
        super(name);
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        var sb = new StringBuilder("CREATE STATISTICS ");
        sb.append(MsDiffUtils.quoteName(name)).append(" ON ").append(parent.getQualifiedName());
        if (!cols.isEmpty()) {
            sb.append(' ');
            StatementUtils.appendCols(sb, cols, getDbType());
        }
        if (filter != null) {
            sb.append("\nWHERE ").append(filter);
        }
        appendOptions(sb);
        script.addStatement(sb);
    }

    private void appendOptions(StringBuilder sb) {
        if (options.isEmpty() && samplePercent == null) {
            return;
        }
        sb.append("\nWITH");
        if (samplePercent != null) {
            sb.append(" SAMPLE ").append(samplePercent).append(", PERSIST_SAMPLE_PERCENT = ON,");
        }
        appendOption(sb, "NORECOMPUTE", " NORECOMPUTE,");
        appendOption(sb, "AUTO_DROP", " AUTO_DROP = ON,");
        appendOption(sb, "INCREMENTAL", " INCREMENTAL = ON,");
        sb.setLength(sb.length() - 1);
    }

    private void appendOption(StringBuilder sb, String condition, String value) {
        if (null == options.get(condition)) {
            return;
        }
        sb.append(value);
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        int startSize = script.getSize();
        var newStat = (MsStatistics) newCondition;
        if (!compareUnalterable(newStat)) {
            return ObjectState.RECREATE;
        }
        if (!Objects.equals(options, newStat.options) || !compareSample(newStat)) {
            StringBuilder sql = new StringBuilder();
            sql.append("UPDATE STATISTICS ")
                    .append(parent.getQualifiedName()).append(" (").append(name).append(")");
            newStat.appendOptions(sql);
            script.addStatement(sql);
        }

        return getObjectState(script, startSize);
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.put(filter);
        hasher.put(cols);
        hasher.put(options);
    }

    @Override
    public boolean compare(IStatement obj) {
        if (this == obj) {
            return true;
        }

        return obj instanceof MsStatistics stat
                && super.compare(stat)
                && compareUnalterable(stat)
                && compareSample(stat)
                && Objects.equals(options, stat.options);
    }

    private boolean compareSample(MsStatistics stat) {
        if (!isParentHasData || !stat.isParentHasData) {
            // MS SQL doesn't persist samplePercent for empty tables - compare only when both have data
            return true;
        }

        return Objects.equals(samplePercent, stat.samplePercent);
    }

    private boolean compareUnalterable(MsStatistics stat) {
        return Objects.equals(filter, stat.filter)
                && Objects.equals(cols, stat.cols);
    }

    @Override
    protected AbstractStatistics getStatisticsCopy() {
        var stat = new MsStatistics(name);
        stat.setFilter(filter);
        stat.setSamplePercent(samplePercent);
        stat.cols.addAll(cols);
        stat.options.putAll(options);
        stat.setParentHasData(isParentHasData);
        return stat;
    }

    @Override
    public DatabaseType getDbType() {
        return DatabaseType.MS;
    }

    @Override
    public ISchema getContainingSchema() {
        return (AbstractSchema) parent.getParent();
    }

    public void setFilter(String filter) {
        this.filter = filter;
        resetHash();
    }

    public void setSamplePercent(String samplePercent) {
        this.samplePercent = samplePercent;
        resetHash();
    }

    public void setParentHasData(boolean parentHasData) {
        this.isParentHasData = parentHasData;
        resetHash();
    }

    /**
     * Adds a column to this statistics object.
     *
     * @param col the column name to add
     */
    public void addCol(String col) {
        cols.add(col);
        resetHash();
    }

    /**
     * Adds an option to this statistics object.
     *
     * @param key the option name
     * @param value the option value
     */
    public void putOption(String key, String value) {
        options.put(key, value);
        resetHash();
    }
}