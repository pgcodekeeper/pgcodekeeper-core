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
 * Represents a Microsoft SQL PRIMARY KEY or UNIQUE constraint.
 * Supports both clustered and non-clustered indexes with various options.
 */
public final class MsConstraintPk extends MsConstraint
        implements IConstraintPk, IOptionContainer, ISimpleColumnContainer {

    private final boolean isPrimaryKey;
    private boolean isClustered;
    private String dataSpace;
    private final List<String> columnNames = new ArrayList<>();
    private final List<SimpleColumn> columns = new ArrayList<>();
    private final Map<String, String> options = new HashMap<>();

    /**
     * Creates a new Microsoft SQL PRIMARY KEY or UNIQUE constraint.
     *
     * @param name         the constraint name
     * @param isPrimaryKey true for PRIMARY KEY, false for UNIQUE
     */
    public MsConstraintPk(String name, boolean isPrimaryKey) {
        super(name);
        this.isPrimaryKey = isPrimaryKey;
    }

    @Override
    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    @Override
    public boolean isClustered() {
        return isClustered;
    }

    public void setClustered(boolean isClustered) {
        this.isClustered = isClustered;
        resetHash();
    }

    public void setDataSpace(String dataSpace) {
        this.dataSpace = dataSpace;
        resetHash();
    }

    @Override
    public List<String> getColumns() {
        return Collections.unmodifiableList(columnNames);
    }

    @Override
    public boolean containsColumn(String name) {
        return columnNames.contains(name);
    }

    @Override
    public void addColumn(SimpleColumn column) {
        columnNames.add(column.getName());
        columns.add(column);
        resetHash();
    }

    /**
     * Throws an exception as include columns are not supported for primary key/unique constraints.
     *
     * @param column the column name (not used)
     * @throws IllegalStateException always, as this operation is unsupported
     */
    @Override
    public void addInclude(String column) {
        throw new IllegalStateException("Unsupported operation");
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

    @Override
    public String getDefinition() {
        final StringBuilder sbSQL = new StringBuilder();
        sbSQL.append(isPrimaryKey ? "PRIMARY KEY " : "UNIQUE ");
        if (!isClustered) {
            sbSQL.append("NON");
        }
        sbSQL.append("CLUSTERED ");
        if (options.keySet().stream().anyMatch("BUCKET_COUNT"::equalsIgnoreCase)) {
            sbSQL.append(" HASH");
        }
        appendSimpleColumns(sbSQL, columns);
        if (!options.isEmpty()) {
            sbSQL.append(" WITH");
            StatementUtils.appendOptionsWithParen(sbSQL, options, " = ");
        }
        if (dataSpace != null) {
            sbSQL.append(" ON ").append(dataSpace);
        }
        return sbSQL.toString();
    }

    private void appendSimpleColumns(StringBuilder sbSQL, List<SimpleColumn> columns) {
        sbSQL.append(" (");
        for (var col : columns) {
            sbSQL.append(getQuotedName(col.getName()));
            if (col.isDesc()) {
                sbSQL.append(" DESC");
            }
            sbSQL.append(", ");
        }
        sbSQL.setLength(sbSQL.length() - 2);
        sbSQL.append(')');
    }

    @Override
    public void computeHash(Hasher hasher) {
        super.computeHash(hasher);
        hasher.put(isPrimaryKey);
        hasher.put(isClustered);
        hasher.put(dataSpace);
        hasher.putOrdered(columns);
        hasher.put(options);
    }

    @Override
    public boolean compare(IStatement obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof MsConstraintPk con && super.compare(obj)) {
            return compareUnalterable(con);
        }

        return false;
    }

    @Override
    protected boolean compareUnalterable(MsConstraint obj) {
        if (obj instanceof MsConstraintPk con) {
            return isPrimaryKey == con.isPrimaryKey
                    && isClustered == con.isClustered
                    && Objects.equals(dataSpace, con.dataSpace)
                    && Objects.equals(columns, con.columns)
                    && Objects.equals(options, con.options);
        }

        return false;
    }

    @Override
    protected MsConstraint getConstraintCopy() {
        var con = new MsConstraintPk(name, isPrimaryKey);
        con.setClustered(isClustered);
        con.setDataSpace(dataSpace);
        con.columnNames.addAll(columnNames);
        con.columns.addAll(columns);
        con.options.putAll(options);
        return con;
    }

    @Override
    public void compareOptions(IOptionContainer newContainer, SQLScript script) {
        // no implementation
    }
}