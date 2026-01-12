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
 **
 * Copyright 2006 StartNet s.r.o.
 *
 * Distributed under MIT license
 *******************************************************************************/
package org.pgcodekeeper.core.database.pg.schema;

import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.utils.Utils;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.database.api.schema.ObjectState;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.List;
import java.util.Objects;

/**
 * PostgreSQL index implementation.
 * Supports all PostgreSQL index features including unique constraints,
 * partial indexes, expression indexes, and index inheritance for partitioned tables.
 */
public final class PgIndex extends AbstractIndex implements IPgStatement {

    private static final String ALTER_INDEX = "ALTER INDEX ";
    private Inherits inherit;
    private String method;
    private boolean nullsDistinction = true;

    /**
     * Creates a new PostgreSQL index.
     *
     * @param name index name
     */
    public PgIndex(String name) {
        super(name);
    }

    @Override
    public boolean canDrop() {
        return inherit == null;
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        getCreationSQL(script, name);
        appendComments(script);
    }

    private void getCreationSQL(SQLScript script, String name) {
        final StringBuilder sbSQL = new StringBuilder();
        sbSQL.append("CREATE ");

        if (unique) {
            sbSQL.append("UNIQUE ");
        }

        sbSQL.append("INDEX ");
        ISettings settings = script.getSettings();
        if (settings.isConcurrentlyMode() && !settings.isAddTransaction()) {
            sbSQL.append("CONCURRENTLY ");
        }
        if (inherit != null || settings.isGenerateExists()) {
            sbSQL.append("IF NOT EXISTS ");
        }
        sbSQL.append(getQuotedName(name))
                .append(" ON ");
        if (parent instanceof PgAbstractRegularTable regTable && regTable.getPartitionBy() != null) {
            sbSQL.append("ONLY ");
        }
        sbSQL.append(parent.getQualifiedName());
        if (method != null) {
            sbSQL.append(" USING ").append(getQuotedName(method));
        }
        appendSimpleColumns(sbSQL, columns);
        appendIndexParam(sbSQL);
        appendWhere(sbSQL);
        script.addStatement(sbSQL);

        if (isClustered) {
            script.addStatement(appendClusterSql());
        }

        if (inherit != null) {
            script.addStatement(ALTER_INDEX + inherit.getQualifiedName() + " ATTACH PARTITION " + getQualifiedName());
        }
    }

    private void appendSimpleColumns(StringBuilder sbSQL, List<SimpleColumn> columns) {
        sbSQL.append(" (");
        for (var col : columns) {
            // column name already quoted
            sbSQL.append(col.getName());
            if (col.getCollation() != null) {
                sbSQL.append(" COLLATE ").append(col.getCollation());
            }
            if (col.getOpClass() != null) {
                sbSQL.append(' ').append(col.getOpClass());
                var opClassParams = col.getOpClassParams();
                if (!opClassParams.isEmpty()) {
                    StatementUtils.appendOptionsWithParen(sbSQL, opClassParams, "=");
                }
            }
            if (col.isDesc()) {
                sbSQL.append(" DESC");
            }
            if (col.getNullsOrdering() != null) {
                sbSQL.append(col.getNullsOrdering());
            }
            sbSQL.append(", ");
        }
        sbSQL.setLength(sbSQL.length() - 2);
        sbSQL.append(')');
    }

    private void appendIndexParam(StringBuilder sb) {
        if (!includes.isEmpty()) {
            sb.append(" INCLUDE ");
            StatementUtils.appendCols(sb, includes, getQuoter());
        }
        if (!nullsDistinction) {
            sb.append(" NULLS NOT DISTINCT");
        }
        if (!options.isEmpty()) {
            sb.append("\nWITH");
            StatementUtils.appendOptionsWithParen(sb, options, "=");
        }
        if (tablespace != null) {
            sb.append("\nTABLESPACE ").append(tablespace);
        }
    }

    @Override
    public String getQualifiedName() {
        if (qualifiedName == null) {
            qualifiedName = getQuotedName(getSchemaName()) + '.' + getQuotedName(name);
        }
        return qualifiedName;
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        int startSize = script.getSize();
        PgIndex newIndex = (PgIndex) newCondition;

        if (!compareUnalterable(newIndex)) {
            if (script.getSettings().isConcurrentlyMode()) {
                // generate optimized command sequence for concurrent index creation
                String tmpName = "tmp" + Utils.getRandom().nextInt(Integer.MAX_VALUE) + "_" + name;
                newIndex.getCreationSQL(script, tmpName);
                script.addStatement("BEGIN TRANSACTION");
                getDropSQL(script);
                StringBuilder sql = new StringBuilder();
                sql.append(ALTER_INDEX)
                        .append(getQuotedName(getSchemaName()))
                        .append('.')
                        .append(getQuotedName(tmpName))
                        .append(" RENAME TO ")
                        .append(getQuotedName(getName()));
                script.addStatement(sql);

                newIndex.appendComments(script);
                script.addStatement("COMMIT TRANSACTION");
            }
            return ObjectState.RECREATE;
        }

        if (!Objects.equals(tablespace, newIndex.tablespace)) {
            StringBuilder sql = new StringBuilder();
            sql.append(ALTER_INDEX).append(newIndex.getQualifiedName())
                    .append(" SET TABLESPACE ");

            String newSpace = newIndex.tablespace;
            sql.append(newSpace == null ? Consts.PG_DEFAULT : newSpace);
            script.addStatement(sql);
        }

        if (newIndex.isClustered != isClustered) {
            if (newIndex.isClustered) {
                script.addStatement(newIndex.appendClusterSql());
            } else if (!((AbstractStatementContainer) newIndex.parent).isClustered()) {
                script.addStatement(ALTER_TABLE + newIndex.parent.getQualifiedName() + " SET WITHOUT CLUSTER");
            }
        }

        compareOptions(newIndex, script);
        appendAlterComments(newIndex, script);

        return getObjectState(script, startSize);
    }

    private String appendClusterSql() {
        return "ALTER " + parent.getTypeName() + ' ' + parent.getQualifiedName() + " CLUSTER ON " + name;
    }

    /**
     * Gets the index access method (btree, hash, gin, gist, etc.).
     *
     * @return index method name
     */
    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
        resetHash();
    }

    /**
     * Sets the parent index for this partitioned index.
     *
     * @param schemaName parent index schema name
     * @param indexName  parent index name
     */
    public void addInherit(final String schemaName, final String indexName) {
        inherit = new Inherits(schemaName, indexName);
        resetHash();
    }

    public void setNullsDistinction(boolean nullsDistinction) {
        this.nullsDistinction = nullsDistinction;
        resetHash();
    }

    @Override
    protected boolean compareUnalterable(AbstractIndex index) {
        if (!(index instanceof PgIndex pgIndex)) {
            return false;
        }
        return super.compareUnalterable(pgIndex)
                && Objects.equals(inherit, pgIndex.inherit)
                && Objects.equals(method, pgIndex.method)
                && nullsDistinction == pgIndex.nullsDistinction;
    }

    @Override
    public void computeHash(Hasher hasher) {
        super.computeHash(hasher);
        hasher.put(inherit);
        hasher.put(method);
        hasher.put(nullsDistinction);
    }

    @Override
    protected AbstractIndex getIndexCopy() {
        PgIndex index = new PgIndex(name);
        index.inherit = inherit;
        index.setMethod(method);
        index.setNullsDistinction(nullsDistinction);
        return index;
    }
}
