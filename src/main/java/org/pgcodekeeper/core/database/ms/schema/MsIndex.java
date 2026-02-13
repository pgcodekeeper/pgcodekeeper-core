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
package org.pgcodekeeper.core.database.ms.schema;

import java.util.*;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;

/**
 * Represents a Microsoft SQL index with support for clustered, non-clustered,
 * and columnstore indexes.
 */
public final class MsIndex extends MsAbstractStatement implements IIndex {

    private boolean isColumnstore;
    private final List<String> orderCols = new ArrayList<>();

    private boolean unique;
    private String tablespace;
    private final List<SimpleColumn> columns = new ArrayList<>();
    private final Map<String, String> options = new LinkedHashMap<>();
    private final List<String> includes = new ArrayList<>();
    private String where;
    private boolean isClustered;

    /**
     * Creates a new Microsoft SQL index.
     *
     * @param name the index name
     */
    public MsIndex(String name) {
        super(name);
    }

    public void setColumnstore(boolean isColumnstore) {
        this.isColumnstore = isColumnstore;
        resetHash();
    }

    /**
     * Adds a column to the ORDER clause for columnstore indexes.
     *
     * @param orderCol the column specification for ordering
     */
    public void addOrderCol(String orderCol) {
        this.orderCols.add(orderCol);
        resetHash();
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        getCreationSQL(script, false);
    }

    private void getCreationSQL(SQLScript script, boolean dropExisting) {
        final StringBuilder sbSQL = new StringBuilder();
        sbSQL.append("CREATE ");
        if (unique) {
            sbSQL.append("UNIQUE ");
        }
        appendClustered(sbSQL);

        if (isColumnstore) {
            sbSQL.append("COLUMNSTORE ");
        }
        sbSQL.append(getDefinition(false, dropExisting, script.getSettings().isConcurrentlyMode()));
        if (tablespace != null) {
            sbSQL.append("\nON ").append(tablespace);
        }
        script.addStatement(sbSQL);
    }

    String getDefinition(boolean isTypeIndex, boolean dropExisting, boolean isConcurrentlyMode) {
        var sb = new StringBuilder();
        sb.append("INDEX ");

        if (!isTypeIndex) {
            appendFullName(sb);
            if (!columns.isEmpty()) {
                sb.append(" (");
                appendSimpleColumns(sb, columns);
                sb.append(')');
            }
        } else {
            sb.append(getQuotedName());
            sb.append(' ');
            appendClustered(sb);
            if (options.keySet().stream().anyMatch("BUCKET_COUNT"::equalsIgnoreCase)) {
                sb.append("HASH");
            }
            sb.append("\n(\n\t");
            appendSimpleColumns(sb, columns);
            sb.append("\n)");
        }

        if (!includes.isEmpty()) {
            sb.append(isColumnstore ? " " : " INCLUDE ");
            StatementUtils.appendCols(sb, includes, getQuoter());
        }
        if (!orderCols.isEmpty()) {
            sb.append("\nORDER ");
            StatementUtils.appendCols(sb, orderCols, getQuoter());
        }
        if (where != null) {
            sb.append("\nWHERE ").append(where);
        }

        var tmpOptions = new LinkedHashMap<>(options);
        if (!isTypeIndex) {
            if (isConcurrentlyMode && !options.containsKey("ONLINE")) {
                tmpOptions.put("ONLINE", "ON");
            }
            if (dropExisting) {
                tmpOptions.put("DROP_EXISTING", "ON");
            }
        }
        if (!tmpOptions.isEmpty()) {
            sb.append(isTypeIndex ? ' ' : '\n').append("WITH (").append(isTypeIndex ? ' ' : "");
            StatementUtils.appendOptions(sb, tmpOptions, " = ");
            sb.append(')');
        }

        return sb.toString();
    }

    private void appendSimpleColumns(StringBuilder sbSQL, List<SimpleColumn> columns) {
        for (var col : columns) {
            sbSQL.append(quote(col.getName()));
            if (col.isDesc()) {
                sbSQL.append(" DESC");
            }
            sbSQL.append(", ");
        }
        sbSQL.setLength(sbSQL.length() - 2);
    }

    private void appendClustered(StringBuilder sb) {
        if (!isClustered) {
            sb.append("NON");
        }
        sb.append("CLUSTERED ");
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        MsIndex newIndex = (MsIndex) newCondition;
        if (!compare(newIndex)) {
            if (!isClustered || newIndex.isClustered) {
                newIndex.getCreationSQL(script, true);
                return ObjectState.ALTER_WITH_DEP;
            }

            return ObjectState.RECREATE;
        }

        // options can be changed by syntax :
        // ALTER INDEX index_name ON schema_name.table REBUILD WITH (options (, option)*)
        // but how to reset option? all indices has all option with default value
        // and we don't know what is it and how to change current value to default value.

        return ObjectState.NOTHING;
    }

    @Override
    public void appendFullName(StringBuilder sb) {
        sb.append(getQuotedName()).append(" ON ").append(parent.getQualifiedName());
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.putOrdered(columns);
        hasher.put(where);
        hasher.put(includes);
        hasher.put(unique);
        hasher.put(isClustered);
        hasher.put(tablespace);
        hasher.put(options);
        hasher.put(isColumnstore);
        hasher.put(orderCols);
    }

    @Override
    public boolean compare(IStatement obj) {
        if (obj instanceof MsIndex index && super.compare(obj)) {
            return compareUnalterable(index)
                    && isClustered == index.isClustered
                    && Objects.equals(tablespace, index.tablespace)
                    && Objects.equals(options, index.options)
                    && isColumnstore == index.isColumnstore
                    && Objects.equals(orderCols, index.orderCols);
        }

        return false;
    }

    private boolean compareUnalterable(MsIndex index) {
        return Objects.equals(columns, index.columns)
                && Objects.equals(where, index.where)
                && Objects.equals(includes, index.includes)
                && unique == index.unique;
    }

    @Override
    protected MsIndex getCopy() {
        MsIndex indexDst = new MsIndex(name);
        indexDst.columns.addAll(columns);
        indexDst.setWhere(where);
        indexDst.includes.addAll(includes);
        indexDst.setUnique(unique);
        indexDst.setClustered(isClustered);
        indexDst.setTablespace(tablespace);
        indexDst.options.putAll(options);
        indexDst.setColumnstore(isColumnstore);
        indexDst.orderCols.addAll(orderCols);
        return indexDst;
    }

    public void setTablespace(String tablespace) {
        this.tablespace = tablespace;
        resetHash();
    }

    public void setWhere(String where) {
        this.where = where;
        resetHash();
    }

    public void setClustered(boolean isClustered) {
        this.isClustered = isClustered;
        resetHash();
    }

    public void setUnique(boolean unique) {
        this.unique = unique;
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

    public boolean isClustered() {
        return isClustered;
    }

    @Override
    public void addColumn(SimpleColumn column) {
        columns.add(column);
        resetHash();
    }

    @Override
    public void addInclude(String include) {
        this.includes.add(include);
        resetHash();
    }

    public String getTablespace() {
        return tablespace;
    }

    @Override
    public boolean isUnique() {
        return unique;
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
}
