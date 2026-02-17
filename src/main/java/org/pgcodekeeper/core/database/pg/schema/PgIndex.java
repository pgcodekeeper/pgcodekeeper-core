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

import java.util.*;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.utils.Utils;

/**
 * PostgreSQL index implementation.
 * Supports all PostgreSQL index features including unique constraints,
 * partial indexes, expression indexes, and index inheritance for partitioned tables.
 */
public class PgIndex extends PgAbstractStatement implements IIndex {

    private static final String ALTER_INDEX = "ALTER INDEX ";

    private final List<SimpleColumn> columns = new ArrayList<>();
    private final List<String> includes = new ArrayList<>();
    private final Map<String, String> options = new LinkedHashMap<>();

    private Inherits inherit;
    private String method;
    private boolean nullsDistinction = true;
    private boolean unique;
    private String where;
    private boolean isClustered;
    private String tablespace;

    /**
     * Creates a new PostgreSQL index.
     *
     * @param name index name
     */
    public PgIndex(String name) {
        super(name);
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        getCreationSQL(script, name);
        appendComments(script);
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
                        .append(quote(getSchemaName()))
                        .append('.')
                        .append(quote(tmpName))
                        .append(" RENAME TO ")
                        .append(getQuotedName());
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
            sql.append(newSpace == null ? PG_DEFAULT : newSpace);
            script.addStatement(sql);
        }

        if (newIndex.isClustered != isClustered) {
            if (newIndex.isClustered) {
                script.addStatement(newIndex.appendClusterSql());
            } else if (!((PgAbstractStatementContainer) newIndex.parent).isClustered()) {
                script.addStatement(ALTER_TABLE + newIndex.parent.getQualifiedName() + " SET WITHOUT CLUSTER");
            }
        }

        compareOptions(newIndex, script);
        appendAlterComments(newIndex, script);

        return getObjectState(script, startSize);
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
        sbSQL.append(getQuotedName())
                .append(" ON ");
        if (parent instanceof PgAbstractRegularTable regTable && regTable.getPartitionBy() != null) {
            sbSQL.append("ONLY ");
        }
        sbSQL.append(parent.getQualifiedName());
        if (method != null) {
            sbSQL.append(" USING ").append(quote(method));
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

    private void appendWhere(StringBuilder sbSQL) {
        if (where != null) {
            sbSQL.append("\nWHERE ").append(where);
        }
    }

    @Override
    public String getQualifiedName() {
        if (qualifiedName == null) {
            qualifiedName = quote(getSchemaName()) + '.' + getQuotedName();
        }
        return qualifiedName;
    }

    private String appendClusterSql() {
        return "ALTER " + parent.getTypeName() + ' ' + parent.getQualifiedName() + " CLUSTER ON " + name;
    }

    @Override
    public boolean canDrop() {
        return inherit == null;
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
    public void addOption(String key, String value) {
        options.put(key, value);
        resetHash();
    }

    @Override
    public Map<String, String> getOptions() {
        return Collections.unmodifiableMap(options);
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

    public boolean isClustered() {
        return isClustered;
    }

    public void setClustered(boolean isClustered) {
        this.isClustered = isClustered;
        resetHash();
    }

    public void setUnique(boolean isUnique) {
        this.unique = isUnique;
        resetHash();
    }

    public void setWhere(String where) {
        this.where = where;
        resetHash();
    }

    public void setTablespace(String tablespace) {
        this.tablespace = tablespace;
        resetHash();
    }

    @Override
    public boolean isUnique() {
        return unique;
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.putOrdered(columns);
        hasher.put(unique);
        hasher.put(where);
        hasher.put(includes);
        hasher.put(inherit);
        hasher.put(method);
        hasher.put(nullsDistinction);
        hasher.put(isClustered);
        hasher.put(tablespace);
        hasher.put(options);
    }

    @Override
    public boolean compare(IStatement obj) {
        if (this == obj) {
            return true;
        }
        return obj instanceof PgIndex index && super.compare(obj)
                && compareUnalterable(index)
                && isClustered == index.isClustered
                && Objects.equals(tablespace, index.tablespace)
                && Objects.equals(options, index.options);
    }

    private boolean compareUnalterable(PgIndex index) {
        return Objects.equals(columns, index.columns)
                && unique == index.unique
                && Objects.equals(where, index.where)
                && Objects.equals(includes, index.includes)
                && Objects.equals(inherit, index.inherit)
                && Objects.equals(method, index.method)
                && nullsDistinction == index.nullsDistinction;
    }

    @Override
    protected PgIndex getCopy() {
        PgIndex copy = new PgIndex(name);
        copy.columns.addAll(columns);
        copy.unique = unique;
        copy.where = where;
        copy.includes.addAll(includes);
        copy.inherit = inherit;
        copy.setMethod(method);
        copy.setNullsDistinction(nullsDistinction);
        copy.isClustered = isClustered;
        copy.tablespace = tablespace;
        copy.options.putAll(options);
        return copy;
    }
}
