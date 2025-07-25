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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.DatabaseType;
import org.pgcodekeeper.core.hashers.Hasher;
import org.pgcodekeeper.core.schema.AbstractConstraint;
import org.pgcodekeeper.core.schema.IConstraintPk;
import org.pgcodekeeper.core.schema.PgStatement;
import org.pgcodekeeper.core.schema.PgStatementContainer;
import org.pgcodekeeper.core.schema.StatementUtils;
import org.pgcodekeeper.core.script.SQLScript;
import org.pgcodekeeper.core.settings.ISettings;

public final class PgConstraintPk extends PgConstraint implements IConstraintPk, PgIndexParamContainer {

    private final boolean isPrimaryKey;
    private boolean isClustered;
    private boolean isDistinct;
    private final List<String> columns = new ArrayList<>();
    private final List<String> includes = new ArrayList<>();
    private final Map<String, String> params = new HashMap<>();
    private String tablespace;

    public PgConstraintPk(String name, boolean isPrimaryKey) {
        super(name);
        this.isPrimaryKey = isPrimaryKey;
    }

    public void setDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
        resetHash();
    }

    @Override
    public void addInclude(String include) {
        includes.add(include);
        resetHash();
    }

    @Override
    public void addParam(String key, String value) {
        params.put(key, value);
        resetHash();
    }

    @Override
    public void setTablespace(String tablespace) {
        this.tablespace = tablespace;
        resetHash();
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

    @Override
    public List<String> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    public void addColumn(String column) {
        columns.add(column);
        resetHash();
    }

    @Override
    public String getErrorCode() {
        return isPrimaryKey ? Consts.INVALID_DEFINITION : Consts.DUPLICATE_RELATION;
    }

    @Override
    public String getDefinition() {
        final StringBuilder sbSQL = new StringBuilder();
        if (isPrimaryKey) {
            sbSQL.append("PRIMARY KEY ");
        } else {
            sbSQL.append("UNIQUE ");
            if (isDistinct) {
                sbSQL.append("NULLS NOT DISTINCT ");
            }
        }
        StatementUtils.appendCols(sbSQL, columns, getDbType());
        appendIndexParam(sbSQL);
        return sbSQL.toString();
    }

    private void appendIndexParam(StringBuilder sb) {
        if (!includes.isEmpty()) {
            sb.append(" INCLUDE ");
            StatementUtils.appendCols(sb, includes, DatabaseType.PG);
        }
        if (!params.isEmpty()) {
            sb.append(" WITH");
            StatementUtils.appendOptionsWithParen(sb, params, getDbType());
        }
        if (tablespace != null) {
            sb.append("\n\tUSING INDEX TABLESPACE ").append(tablespace);
        }
    }

    @Override
    protected void appendExtraOptions(StringBuilder sbSQL) {
        if (isClustered()) {
            sbSQL.append("\n\n");
            appendAlterTable(sbSQL);
            sbSQL.append(" CLUSTER ON ").append(name).append(";");
        }
    }

    @Override
    protected void compareExtraOptions(PgConstraint obj, SQLScript script) {
        PgConstraintPk newConstr = (PgConstraintPk) obj;

        if (newConstr.isClustered() != isClustered()) {
            StringBuilder sb = new StringBuilder();
            if (newConstr.isClustered()) {
                appendAlterTable(sb);
                sb.append(" CLUSTER ON ").append(name);
                script.addStatement(sb);
            } else if (!((PgStatementContainer) newConstr.parent).isClustered()) {
                appendAlterTable(sb);
                sb.append(" SET WITHOUT CLUSTER");
                script.addStatement(sb);
            }
        }
    }

    @Override
    protected boolean isGenerateNotValid(ISettings settings) {
        return false;
    }

    @Override
    public boolean compare(PgStatement obj) {
        if (obj instanceof PgConstraintPk con && super.compare(con)) {
            return compareUnalterable(con)
                    && isClustered() == con.isClustered();
        }
        return false;
    }

    @Override
    protected boolean compareUnalterable(PgConstraint newConstr) {
        var con = (PgConstraintPk) newConstr;
        return super.compareUnalterable(con)
                && isPrimaryKey == con.isPrimaryKey
                && isDistinct == con.isDistinct
                && Objects.equals(columns, con.columns)
                && Objects.equals(includes, con.includes)
                && Objects.equals(params, con.params)
                && Objects.equals(tablespace, con.tablespace);
    }

    @Override
    public void computeHash(Hasher hasher) {
        super.computeHash(hasher);
        hasher.put(isPrimaryKey);
        hasher.put(isClustered);
        hasher.put(isDistinct);
        hasher.put(columns);
        hasher.put(includes);
        hasher.put(params);
        hasher.put(tablespace);
    }

    @Override
    protected AbstractConstraint getConstraintCopy() {
        var con = new PgConstraintPk(name, isPrimaryKey);
        con.setClustered(isClustered);
        con.setDistinct(isDistinct);
        con.columns.addAll(columns);
        con.includes.addAll(includes);
        con.params.putAll(params);
        con.setTablespace(tablespace);
        return con;
    }
}