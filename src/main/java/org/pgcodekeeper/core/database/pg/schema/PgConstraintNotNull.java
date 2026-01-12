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
import org.pgcodekeeper.core.database.pg.PgDiffUtils;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.database.base.schema.AbstractConstraint;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.script.SQLScript;

/**
 * PostgreSQL NOT NULL constraint implementation.
 * NOT NULL constraints ensure that a column cannot contain NULL values.
 * Since PostgreSQL 18, NOT NULL constraints can be named and support NO INHERIT option.
 */
public class PgConstraintNotNull extends PgConstraint {

    private static final String NOT_NULL = "NOT NULL";
    public static final String NO_INHERIT = " NO INHERIT";
    private boolean isNoInherit;
    private String defaultName;

    public PgConstraintNotNull(String name) {
        super(name);
    }

    public PgConstraintNotNull(String tableName, String columnName) {
        super(PgDiffUtils.getDefaultObjectName(tableName, columnName, "not_null"));
        this.defaultName = name;
    }

    @Override
    public String getDefinition() {
        var sb = new StringBuilder(NOT_NULL);
        sb.append(" ").append(getQuotedName(getParent().getName()));
        if (isNoInherit){
            sb.append(NO_INHERIT);
        }

        return sb.toString();
    }

    public void getDefinitionForColumn(StringBuilder sb) {
        if (hasCustomName() || comment != null) {
            sb.append(" CONSTRAINT ").append(getQuotedName(name));
        }
        sb.append(" ").append(NOT_NULL);
        if (isNoInherit) {
            sb.append(NO_INHERIT);
        }
    }

    @Override
    public String getRenameCommand(String newName) {
        var sb = new StringBuilder();
        appendAlterTable(sb);
        sb.append("\n\tRENAME CONSTRAINT ").append(getQuotedName(name)).append(" TO ").append(getQuotedName(newName));
        return sb.toString();
    }

    @Override
    protected void compareExtraOptions(PgConstraint obj, SQLScript script) {
        PgConstraintNotNull newConstr = (PgConstraintNotNull) obj;
        var newNoInherit = newConstr.isNoInherit;
        if (isNoInherit == newNoInherit) {
            return;
        }

        var sb = new StringBuilder();
        appendAlterTable(sb);
        sb.append("\n\tALTER CONSTRAINT ");
        sb.append(getQuotedName(newConstr.name));
        sb.append(newNoInherit ? NO_INHERIT : " INHERIT");
        script.addStatement(sb);
    }

    public void setNoInherit(boolean noInherit) {
        isNoInherit = noInherit;
        resetHash();
    }

    @Override
    public String getTableName() {
        return getTable().getName();
    }

    private String getColumnName() {
        return getParent().getName();
    }

    private boolean hasCustomName() {
        return !name.equals(getDefaultName());
    }

    private String getDefaultName() {
        if (defaultName == null) {
            defaultName = PgDiffUtils.getDefaultObjectName(getTableName(), getColumnName(), "not_null");
        }
        return defaultName;
    }

    /**
     * @return true if the constraint can only be written via ALTER TABLE
     */
    public boolean isComplexNotNull() {
        return comment != null || isNotValid || isNoInherit || hasCustomName();
    }

    @Override
    protected void appendAlterTable(StringBuilder sb) {
        sb.append("ALTER TABLE ").append(getTable().getQualifiedName());
    }

    private AbstractStatement getTable() {
        return getParent().getParent();
    }

    @Override
    protected void appendCommentSql(SQLScript script) {
        String sb = "COMMENT ON CONSTRAINT " +
                getQuotedName(name) + " ON " +
                getTable().getQualifiedName() + " IS " + (checkComments() ? comment : "NULL");
        script.addCommentStatement(sb);
    }

    @Override
    public boolean compare(IStatement obj) {
        if (obj instanceof PgConstraintNotNull con && super.compare(con)) {
            return compareUnalterable(con) && isNoInherit == con.isNoInherit;
        }

        return false;
    }

    @Override
    public void computeHash(Hasher hasher) {
        super.computeHash(hasher);
        hasher.put(isNoInherit);
    }

    @Override
    protected AbstractConstraint getConstraintCopy() {
        var con = new PgConstraintNotNull(name);
        con.setNoInherit(isNoInherit);
        return con;
    }

    @Override
    public String getErrorCode() {
        return Consts.DUPLICATE_OBJECT;
    }
}
