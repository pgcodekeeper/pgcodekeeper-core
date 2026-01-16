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

import java.util.Objects;

import org.pgcodekeeper.core.database.api.schema.IPartitionTable;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;

/**
 * Partition foreign table object for PostgreSQL.
 * Represents a partition of a foreign table, which allows partitioning
 * of data across foreign servers while maintaining the partitioning structure.
 *
 * @author galiev_mr
 * @since 4.1.1
 */
public final class PgPartitionForeignTable extends PgAbstractForeignTable implements IPartitionTable {

    private final String partitionBounds;

    /**
     * Creates a new partition foreign table.
     *
     * @param name            table name
     * @param serverName      foreign server name
     * @param partitionBounds partition bounds definition
     */
    public PgPartitionForeignTable(String name, String serverName, String partitionBounds) {
        super(name, serverName);
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
    protected boolean isNeedRecreate(AbstractTable newTable) {
        return super.isNeedRecreate(newTable)
                || !(Objects.equals(partitionBounds, ((PgPartitionForeignTable) newTable).partitionBounds))
                || !inherits.equals(((PgAbstractTable) newTable).inherits);
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
    protected void appendInherit(StringBuilder sb) {
        // PgTable.inherits stores PARTITION OF table in this implementation
    }

    @Override
    protected AbstractTable getTableCopy() {
        return new PgPartitionForeignTable(name, serverName, partitionBounds);
    }

    @Override
    protected boolean compareTable(AbstractStatement obj) {
        return obj instanceof PgPartitionForeignTable table
                && super.compareTable(table)
                && Objects.equals(partitionBounds, table.partitionBounds);
    }

    @Override
    public void computeHash(Hasher hasher) {
        super.computeHash(hasher);
        hasher.put(partitionBounds);
    }
}