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
package org.pgcodekeeper.core.schema.pg;

import java.util.Map.Entry;

import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.PgDiffUtils;
import org.pgcodekeeper.core.hashers.Hasher;
import org.pgcodekeeper.core.schema.AbstractConstraint;
import org.pgcodekeeper.core.schema.AbstractTable;
import org.pgcodekeeper.core.schema.ISimpleOptionContainer;
import org.pgcodekeeper.core.schema.PgStatement;
import org.pgcodekeeper.core.script.SQLActionType;
import org.pgcodekeeper.core.script.SQLScript;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.Objects;

/**
 * Base implementation of regular table
 *
 * @since 4.1.1
 * @author galiev_mr
 *
 */
public abstract class AbstractRegularTable extends AbstractPgTable implements ISimpleOptionContainer {

    private boolean isLogged = true;
    private String tablespace;
    private boolean isRowSecurity;
    private boolean isForceSecurity;
    private String partitionBy;
    private String distribution;
    private String method = Consts.HEAP;

    protected AbstractRegularTable(String name) {
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
    public String getAlterTable(boolean only) {
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
            sql.append("\nUSING ").append(PgDiffUtils.getQuotedName(method));
        }

        StringBuilder sb = new StringBuilder();
        for (Entry <String, String> entry : options.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            sb.append(key);
            if (!value.isEmpty()) {
                sb.append('=').append(value);
            }
            sb.append(", ");
        }

        if (hasOids) {
            sb.append("OIDS=").append(hasOids).append(", ");
        }

        if (sb.length() > 0){
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
    protected void compareTableOptions(AbstractPgTable newTable, SQLScript script) {
        super.compareTableOptions(newTable, script);

        AbstractRegularTable newRegTable = (AbstractRegularTable) newTable;
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
    private void appendDefaultDistribution(AbstractRegularTable newTable, StringBuilder sql) {
        sql.append("DISTRIBUTED ");

        String columnName = null;
        // search DISTRIBUTED column(s)
        // 1 step - search in primary key
        for (AbstractConstraint constraint : newTable.getConstraints()) {
            if (constraint.isPrimaryKey()) {
                columnName = String.join(", ", constraint.getColumns());
                break;
            }
        }

        // 2 step - search in columns list
        if (columnName == null) {
            for (var column : newTable.getColumns()) {
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
    protected boolean isNeedRecreate(AbstractTable newTable) {
        if (super.isNeedRecreate(newTable)) {
            return true;
        }

        if (!(newTable instanceof AbstractRegularTable regTable)) {
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

    public String getPartitionBy() {
        return partitionBy;
    }

    public void setPartitionBy(final String partionBy) {
        this.partitionBy = partionBy;
        resetHash();
    }

    public void setDistribution(String distribution) {
        this.distribution = distribution;
        resetHash();
    }

    @Override
    public boolean compare(PgStatement obj) {
        if (obj instanceof AbstractRegularTable table && super.compare(obj)) {
            return Objects.equals(tablespace, table.tablespace)
                    && isLogged == table.isLogged
                    && isRowSecurity == table.isRowSecurity
                    && isForceSecurity == table.isForceSecurity
                    && Objects.equals(partitionBy, table.partitionBy)
                    && Objects.equals(distribution, table.distribution)
                    && Objects.equals(method, table.method);
        }

        return false;
    }

    @Override
    public AbstractTable shallowCopy() {
        AbstractRegularTable copy = (AbstractRegularTable) super.shallowCopy();
        copy.setLogged(isLogged);
        copy.setTablespace(tablespace);
        copy.setRowSecurity(isRowSecurity);
        copy.setForceSecurity(isForceSecurity);
        copy.setPartitionBy(partitionBy);
        copy.setDistribution(distribution);
        copy.setMethod(method);
        return copy;
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
}
