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

import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.database.api.schema.ObjectState;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;

/**
 * Represents a Microsoft SQL index with support for clustered, non-clustered,
 * and columnstore indexes.
 */
public final class MsIndex extends AbstractIndex implements IMsStatement {

    private boolean isColumnstore;
    private final List<String> orderCols = new ArrayList<>();

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
            sb.append(getQuotedName(name));
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
        appendWhere(sb);

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
            sbSQL.append(getQuotedName(col.getName()));
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
    protected void appendFullName(StringBuilder sb) {
        sb.append(getQuotedName(name)).append(" ON ").append(parent.getQualifiedName());
    }

    @Override
    public boolean compare(IStatement obj) {
        if (obj instanceof MsIndex ind && super.compare(obj)) {
            return isColumnstore == ind.isColumnstore
                    && Objects.equals(orderCols, ind.orderCols);
        }

        return false;
    }

    @Override
    public void computeHash(Hasher hasher) {
        super.computeHash(hasher);
        hasher.put(isColumnstore);
        hasher.put(orderCols);
    }

    @Override
    protected AbstractIndex getIndexCopy() {
        MsIndex ind = new MsIndex(name);
        ind.setColumnstore(isColumnstore);
        ind.orderCols.addAll(orderCols);
        return ind;
    }
}
