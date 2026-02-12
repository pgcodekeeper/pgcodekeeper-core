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

import java.util.Map.Entry;
import java.util.Objects;

import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.database.api.schema.ISimpleOptionContainer;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.*;
import org.pgcodekeeper.core.settings.ISettings;

/**
 * Base PostgreSQL regular table implementation.
 * Provides common functionality for standard PostgreSQL tables including
 * logging, tablespace, row-level security, partitioning, and Greenplum distribution options.
 *
 * @author galiev_mr
 * @since 4.1.1
 */
public abstract class PgAbstractRegularTable extends PgAbstractTable implements ISimpleOptionContainer {

    private boolean isLogged = true;
    private String tablespace;
    private boolean isRowSecurity;
    private boolean isForceSecurity;
    private String partitionBy;
    private String distribution;
    private String method = Consts.HEAP;

    /**
     * Creates a new PostgreSQL regular table.
     *
     * @param name table name
     */
    protected PgAbstractRegularTable(String name) {
        super(name);
    }

    @Override
    protected void appendName(StringBuilder sbSQL, ISettings settings) {
        sbSQL.append("CREATE ");
        if (!isLogged) {
            sbSQL.append("UNLOGGED ");
        }
        sbSQL.append("TABLE ");
        appendIfNotExists(sbSQL, settings);
        sbSQL.append(getQualifiedName());
    }

    @Override
    protected String getAlterTable(boolean only) {
        StringBuilder sb = new StringBuilder();
        sb.append(ALTER_TABLE);
        if (only) {
            sb.append("ONLY ");
        }
        sb.append(getQualifiedName());
        return sb.toString();
    }

    @Override
    protected void appendOptions(StringBuilder sql) {
        if (partitionBy != null) {
            sql.append("\nPARTITION BY ");
            sql.append(partitionBy);
        }

        if (!Consts.HEAP.equals(method)) {
            sql.append("\nUSING ").append(getQuotedName(method));
        }

        StringBuilder sb = new StringBuilder();
        for (Entry<String, String> entry : options.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            sb.append(key);
            if (!value.isEmpty()) {
                sb.append('=').append(value);
            }
            sb.append(", ");
        }

        if (hasOids) {
            sb.append("OIDS=").append(true).append(", ");
        }

        if (!sb.isEmpty()) {
            sb.setLength(sb.length() - 2);
            sql.append("\nWITH (").append(sb).append(")");
        }

        if (tablespace != null && !tablespace.isEmpty()) {
            sql.append("\nTABLESPACE ");
            sql.append(tablespace);
        }

        if (distribution != null) {
            sql.append("\n").append(distribution);
        }
    }

    @Override
    protected void appendAlterOptions(SQLScript script) {
        // since 9.5 PostgreSQL
        if (isRowSecurity) {
            script.addStatement(getRowLevel(" ENABLE ", false));
        }

        // since 9.5 PostgreSQL
        if (isForceSecurity) {
            script.addStatement(getRowLevel(" FORCE ", true));
        }
    }

    private String getRowLevel(String securityType, boolean only) {
        return getAlterTable(only) + securityType + "ROW LEVEL SECURITY";
    }

    public void setMethod(String using) {
        this.method = using;
        resetHash();
    }

    @Override
    protected void compareTableOptions(PgAbstractTable newTable, SQLScript script) {
        super.compareTableOptions(newTable, script);

        PgAbstractRegularTable newRegTable = (PgAbstractRegularTable) newTable;
        if (!Objects.equals(tablespace, newRegTable.tablespace)) {
            StringBuilder sql = new StringBuilder();

            sql.append(getAlterTable(false))
                    .append("\n\tSET TABLESPACE ");

            String newSpace = newRegTable.tablespace;
            sql.append(newSpace == null ? Consts.PG_DEFAULT : newSpace);
            script.addStatement(sql);
        }

        // since 9.5 PostgreSQL
        if (isLogged != newRegTable.isLogged) {
            StringBuilder sql = new StringBuilder();
            sql.append(getAlterTable(false))
                    .append("\n\tSET ")
                    .append(newRegTable.isLogged ? "LOGGED" : "UNLOGGED");
            script.addStatement(sql);
        }

        // since 9.5 PostgreSQL
        if (isRowSecurity != newRegTable.isRowSecurity) {
            StringBuilder sql = new StringBuilder();
            sql.append(getAlterTable(false))
                    .append(newRegTable.isRowSecurity ? " ENABLE" : " DISABLE")
                    .append(" ROW LEVEL SECURITY");
            script.addStatement(sql);
        }

        // since 9.5 PostgreSQL
        if (isForceSecurity != newRegTable.isForceSecurity) {
            StringBuilder sql = new StringBuilder();
            sql.append(getAlterTable(true))
                    .append(newRegTable.isForceSecurity ? "" : " NO")
                    .append(" FORCE ROW LEVEL SECURITY");
            script.addStatement(sql);
        }

        // greenplum
        if (!Objects.equals(distribution, newRegTable.distribution)) {
            StringBuilder sql = new StringBuilder();
            sql.append(getAlterTable(false));
            sql.append(" SET ");
            String newDistribution = newRegTable.distribution;
            if (distribution != null && distribution.startsWith("DISTRIBUTED BY")
                    && (newDistribution == null || !newDistribution.startsWith("DISTRIBUTED REPLICATED"))) {
                sql.append("WITH (REORGANIZE=true) ");
            }

            if (newDistribution != null) {
                sql.append(newDistribution);
            } else {
                appendDefaultDistribution(newRegTable, sql);
            }
            script.addStatement(sql.toString(), SQLActionType.END);
        }
    }

    /**
     * append default value for greenplum db DISTRIBUTED clause
     */
    private void appendDefaultDistribution(PgAbstractRegularTable newTable, StringBuilder sql) {
        sql.append("DISTRIBUTED ");

        String columnName = null;
        // search DISTRIBUTED column(s)
        // 1 step - search in primary key
        for (PgConstraint constraint : newTable.getConstraints()) {
            if (constraint.isPrimaryKey()) {
                columnName = String.join(", ", constraint.getColumns());
                break;
            }
        }

        // 2 step - search in columns list
        if (columnName == null) {
            for (var column : newTable.columns) {
                if (!column.getType().contains(".")) {
                    columnName = column.getName();
                    break;
                }
            }
        }

        if (columnName != null) {
            sql.append("BY (").append(columnName).append(")");
        } else {
            sql.append("RANDOMLY");
        }
    }

    protected abstract void convertTable(SQLScript script);

    @Override
    protected boolean isNeedRecreate(PgAbstractTable newTable) {
        if (super.isNeedRecreate(newTable)) {
            return true;
        }

        if (!(newTable instanceof PgAbstractRegularTable regTable)) {
            return true;
        }

        return !Objects.equals(method, regTable.method)
                || !Objects.equals(partitionBy, regTable.partitionBy);
    }

    @Override
    protected PgSequence writeSequences(PgColumn column, StringBuilder sbOption) {
        PgSequence sequence = super.writeSequences(column, sbOption);
        if (isLogged != sequence.isLogged()) {
            sbOption.append("\nALTER SEQUENCE ").append(sequence.getQualifiedName()).append(" SET ")
                    .append(sequence.isLogged() ? "LOGGED;" : "UNLOGGED;");
        }
        return sequence;
    }

    /**
     * Checks if this table is logged (writes to WAL).
     *
     * @return true if logged, false if unlogged
     */
    public boolean isLogged() {
        return isLogged;
    }

    public void setLogged(boolean isLogged) {
        this.isLogged = isLogged;
        resetHash();
    }

    public void setTablespace(final String tablespace) {
        this.tablespace = tablespace;
        resetHash();
    }

    public void setRowSecurity(final boolean isRowSecurity) {
        this.isRowSecurity = isRowSecurity;
        resetHash();
    }

    public void setForceSecurity(final boolean isForceSecurity) {
        this.isForceSecurity = isForceSecurity;
        resetHash();
    }

    /**
     * Gets the partition specification for this table.
     *
     * @return partition by clause or null if not partitioned
     */
    public String getPartitionBy() {
        return partitionBy;
    }

    public void setPartitionBy(final String partitionBy) {
        this.partitionBy = partitionBy;
        resetHash();
    }

    public void setDistribution(String distribution) {
        this.distribution = distribution;
        resetHash();
    }

    @Override
    public void computeHash(Hasher hasher) {
        super.computeHash(hasher);
        hasher.put(isLogged);
        hasher.put(tablespace);
        hasher.put(isRowSecurity);
        hasher.put(isForceSecurity);
        hasher.put(partitionBy);
        hasher.put(distribution);
        hasher.put(method);
    }

    @Override
    protected boolean compareTable(PgAbstractTable obj) {
        return obj instanceof PgAbstractRegularTable table
                && super.compareTable(table)
                && isLogged == table.isLogged
                && Objects.equals(tablespace, table.tablespace)
                && isRowSecurity == table.isRowSecurity
                && isForceSecurity == table.isForceSecurity
                && Objects.equals(partitionBy, table.partitionBy)
                && Objects.equals(distribution, table.distribution)
                && Objects.equals(method, table.method);
    }

    @Override
    protected PgAbstractTable getCopy() {
        PgAbstractRegularTable copy = (PgAbstractRegularTable) super.getCopy();
        copy.setLogged(isLogged);
        copy.setTablespace(tablespace);
        copy.setRowSecurity(isRowSecurity);
        copy.setForceSecurity(isForceSecurity);
        copy.setPartitionBy(partitionBy);
        copy.setDistribution(distribution);
        copy.setMethod(method);
        return copy;
    }
}
