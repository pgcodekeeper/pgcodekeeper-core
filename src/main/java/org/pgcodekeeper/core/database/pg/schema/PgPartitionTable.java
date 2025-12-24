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
package org.pgcodekeeper.core.database.pg.schema;

import org.pgcodekeeper.core.database.pg.PgDiffUtils;
import org.pgcodekeeper.core.database.api.schema.IPartitionTable;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.database.base.schema.AbstractColumn;
import org.pgcodekeeper.core.database.base.schema.AbstractTable;
import org.pgcodekeeper.core.database.base.schema.Inherits;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;

import java.util.List;
import java.util.Objects;

/**
 * Partition regular table object for PostgreSQL.
 * Represents a table partition that is part of a larger partitioned table,
 * implementing PostgreSQL's native table partitioning functionality.
 *
 * @author galiev_mr
 * @since 4.1.1
 */
public final class PgPartitionTable extends PgAbstractRegularTable implements IPartitionTable {

    private final String partitionBounds;

    /**
     * Creates a new partition table.
     *
     * @param name            table name
     * @param partitionBounds partition bounds definition
     */
    public PgPartitionTable(String name, String partitionBounds) {
        super(name);
        this.partitionBounds = partitionBounds;
    }

    @Override
    public String getPartitionBounds() {
        return partitionBounds;
    }

    @Override
    public String getParentTable() {
        return inherits.get(0).getQualifiedName();
    }

    @Override
    protected void appendColumns(StringBuilder sbSQL, SQLScript script) {
        sbSQL.append(" PARTITION OF ").append(getParentTable());

        if (!columns.isEmpty()) {
            sbSQL.append(" (\n");

            int start = sbSQL.length();
            for (AbstractColumn column : columns) {
                writeColumn((PgColumn) column, sbSQL, script);
            }

            if (start != sbSQL.length()) {
                sbSQL.setLength(sbSQL.length() - 2);
                sbSQL.append("\n)");
            } else {
                sbSQL.setLength(sbSQL.length() - 3);
            }
        }

        sbSQL.append('\n');
        sbSQL.append(partitionBounds);
    }


    @Override
    protected void appendInherit(StringBuilder sbSQL) {
        // PgTable.inherits stores PARTITION OF table in this implementation
    }

    @Override
    protected void compareTableTypes(PgAbstractTable newTable, SQLScript script) {
        if (!(newTable instanceof PgPartitionTable)) {
            script.addStatement(appendTablePartiton(getParentTable(), "DETACH"));

            if (newTable instanceof PgAbstractRegularTable table) {
                table.convertTable(script);
            }
        }
    }

    @Override
    protected void convertTable(SQLScript script) {
        Inherits newInherits = inherits.get(0);
        StringBuilder sql = appendTablePartiton(newInherits.getQualifiedName(), "ATTACH");
        sql.append(' ').append(partitionBounds);
        script.addStatement(sql);
    }

    private StringBuilder appendTablePartiton(String tableName, String state) {
        return new StringBuilder(ALTER_TABLE).append(tableName)
                .append("\n\t%s PARTITION ".formatted(state))
                .append(PgDiffUtils.getQuotedName(parent.getName()))
                .append('.')
                .append(PgDiffUtils.getQuotedName(getName()));
    }

    @Override
    protected void compareTableOptions(PgAbstractTable newTable, SQLScript script) {
        super.compareTableOptions(newTable, script);

        if (newTable instanceof PgPartitionTable table) {

            Inherits oldInherits = inherits.get(0);
            Inherits newInherits = newTable.inherits.get(0);

            if (!Objects.equals(partitionBounds, table.partitionBounds)
                    || !Objects.equals(oldInherits, newInherits)) {
                script.addStatement(appendTablePartiton(oldInherits.getQualifiedName(), "DETACH"));
                StringBuilder sql = appendTablePartiton(newInherits.getQualifiedName(), "ATTACH");
                sql.append(' ').append(table.getPartitionBounds());
                script.addStatement(sql);
            }
        }
    }

    @Override
    protected void compareInherits(PgAbstractTable newTable, SQLScript script) {
        //not support default syntax
    }

    @Override
    protected AbstractTable getTableCopy() {
        return new PgPartitionTable(name, partitionBounds);
    }

    @Override
    protected boolean compareTable(AbstractStatement obj) {
        return obj instanceof PgPartitionTable table
                && super.compareTable(table)
                && Objects.equals(partitionBounds, table.partitionBounds);
    }

    @Override
    public void computeHash(Hasher hasher) {
        super.computeHash(hasher);
        hasher.put(partitionBounds);
    }

    @Override
    public void appendMoveDataSql(IStatement newCondition, SQLScript script, String tblTmpBareName,
                                  List<String> identityCols) {
        // no impl
    }
}