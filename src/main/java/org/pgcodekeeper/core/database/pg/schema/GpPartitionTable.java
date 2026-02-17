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

import java.util.*;

import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;

/**
 * Greenplum partition table implementation.
 * Represents a partitioned table specific to Greenplum database
 * with support for templates and Greenplum-specific partitioning features.
 */
public class GpPartitionTable extends PgAbstractRegularTable {

    private final Map<String, GpPartitionTemplateContainer> templates = new HashMap<>();

    private String partitionGpBounds;
    private String normalizedPartitionGpBounds;

    /**
     * Creates a new Greenplum partition table.
     *
     * @param name table name
     */
    public GpPartitionTable(String name) {
        super(name);
    }

    @Override
    protected void appendColumns(StringBuilder sbSQL, SQLScript script) {
        sbSQL.append(" (\n");

        int start = sbSQL.length();
        for (PgColumn column : columns) {
            writeColumn(column, sbSQL, script);
        }

        if (start != sbSQL.length()) {
            sbSQL.setLength(sbSQL.length() - 2);
            sbSQL.append('\n');
        }

        sbSQL.append(')');
    }

    @Override
    protected boolean isNeedRecreate(PgAbstractTable newTable) {
        return super.isNeedRecreate(newTable) || !this.getClass().equals(newTable.getClass());
    }

    @Override
    protected void appendOptions(StringBuilder sbSQL) {
        super.appendOptions(sbSQL);
        sbSQL.append("\n").append(partitionGpBounds);
    }

    @Override
    protected void appendAlterOptions(SQLScript script) {
        super.appendAlterOptions(script);
        for (var template : templates.values()) {
            StringBuilder sql = new StringBuilder();
            sql.append(getAlterTable(false));
            template.appendCreateSQL(sql);
            script.addStatement(sql);
        }
    }

    @Override
    protected void compareTableOptions(PgAbstractTable newTable, SQLScript script) {
        super.compareTableOptions(newTable, script);

        GpPartitionTable newPartGpTable = (GpPartitionTable) newTable;
        if (!Objects.equals(normalizedPartitionGpBounds, newPartGpTable.normalizedPartitionGpBounds)) {
            script.addStatement("\n --The PARTTITION clause have differences. Add ALTER statement manually");
        }
        compareTemplates(newPartGpTable, script);
    }

    private void compareTemplates(GpPartitionTable newTable, SQLScript script) {
        if (Objects.equals(templates, newTable.templates)) {
            return;
        }

        templates.forEach((key, value) -> {
            GpPartitionTemplateContainer newValue = newTable.templates.get(key);
            StringBuilder sql;
            if (newValue == null) {
                sql = new StringBuilder();
                sql.append(getAlterTable(false));
                value.appendDropSql(sql);
                script.addStatement(sql);
            } else if (!value.equals(newValue)) {
                sql = new StringBuilder();
                sql.append(getAlterTable(false));
                newValue.appendCreateSQL(sql);
                script.addStatement(sql);
            }
        });

        newTable.templates.forEach((key, value) -> {
            if (!templates.containsKey(key)) {
                StringBuilder sql = new StringBuilder();
                sql.append(getAlterTable(false));
                value.appendCreateSQL(sql);
                script.addStatement(sql);
            }
        });
    }

    @Override
    protected void compareTableTypes(PgAbstractTable newTable, SQLScript script) {
        // no implements
    }

    @Override
    protected void convertTable(SQLScript script) {
        // available in 7 version
    }

    /**
     * Sets the partition bounds for this Greenplum table.
     *
     * @param partitionGpBounds           raw partition bounds
     * @param normalizedPartitionGpBounds normalized partition bounds for comparison
     */
    public void setPartitionGpBound(String partitionGpBounds, String normalizedPartitionGpBounds) {
        this.partitionGpBounds = partitionGpBounds;
        this.normalizedPartitionGpBounds = normalizedPartitionGpBounds;
        resetHash();
    }

    /**
     * Adds a partition template to this table.
     *
     * @param template partition template to add
     */
    public void addTemplate(GpPartitionTemplateContainer template) {
        this.templates.put(template.getPartitionName(), template);
        resetHash();
    }

    @Override
    public String getPartitionBy() {
        return normalizedPartitionGpBounds;
    }


    @Override
    public void computeHash(Hasher hasher) {
        super.computeHash(hasher);
        hasher.putUnordered(templates);
        hasher.put(normalizedPartitionGpBounds);
    }

    @Override
    protected boolean compareTable(PgAbstractTable obj) {
        if (this == obj) {
            return true;
        }
        return obj instanceof GpPartitionTable table
                && super.compareTable(table)
                && Objects.equals(templates, table.templates)
                && Objects.equals(normalizedPartitionGpBounds, table.normalizedPartitionGpBounds);
    }

    @Override
    protected PgAbstractTable getTableCopy() {
        GpPartitionTable copy = new GpPartitionTable(name);
        copy.templates.putAll(templates);
        copy.setPartitionGpBound(partitionGpBounds, normalizedPartitionGpBounds);
        return copy;
    }
}
