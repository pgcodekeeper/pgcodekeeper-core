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

import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.database.api.formatter.IFormatConfiguration;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.api.schema.IPrivilege;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.database.pg.PgDiffUtils;
import org.pgcodekeeper.core.database.pg.formatter.PgFormatter;
import org.pgcodekeeper.core.script.SQLScript;

import java.util.*;
import java.util.function.UnaryOperator;

public abstract class PgAbstractStatement extends AbstractStatement {

    private static final String RENAME_OBJECT_COMMAND = "ALTER %s %s RENAME TO %s";

    protected PgAbstractStatement(String name) {
        super(name);
    }

    @Override
    public String formatSql(String sql, int offset, int length, IFormatConfiguration formatConfiguration) {
        return new PgFormatter(sql, offset, length, formatConfiguration).formatText();
    }

    @Override
    public UnaryOperator<String> getQuoter() {
        return PgDiffUtils::getQuotedName;
    }

    @Override
    public void appendOwnerSQL(SQLScript script) {
        var owner = getOwner();
        if (owner == null || !isOwned()) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER ").append(getTypeName()).append(' ');
        appendFullName(sb);
        sb.append(" OWNER TO ").append(quote(owner));

        script.addStatement(sb);
    }

    @Override
    protected void alterPrivileges(AbstractStatement newObj, SQLScript script) {
        Set<IPrivilege> newPrivileges = newObj.getPrivileges();

        // if new object has all privileges from old object and if it doesn't have
        // new revokes, then we can just grant difference between new and old privileges
        if (newPrivileges.containsAll(privileges) && Objects.equals(owner, newObj.getOwner())) {
            Set<IPrivilege> diff = new LinkedHashSet<>(newPrivileges);
            diff.removeAll(privileges);
            boolean isGrantOnly = diff.stream().noneMatch(IPrivilege::isRevoke);
            if (isGrantOnly) {
                IPrivilege.appendPrivileges(diff, script);
                return;
            }
        }
        super.alterPrivileges(newObj, script);
    }

    @Override
    public boolean isOwned() {
        return getStatementType().in(DbObjType.FOREIGN_DATA_WRAPPER, DbObjType.SERVER, DbObjType.EVENT_TRIGGER,
                DbObjType.FTS_CONFIGURATION, DbObjType.FTS_DICTIONARY, DbObjType.TABLE, DbObjType.VIEW, DbObjType.SCHEMA,
                DbObjType.FUNCTION, DbObjType.OPERATOR, DbObjType.PROCEDURE, DbObjType.AGGREGATE, DbObjType.SEQUENCE,
                DbObjType.COLLATION, DbObjType.TYPE, DbObjType.DOMAIN, DbObjType.STATISTICS);
    }

    @Override
    public void appendDefaultPrivileges(IStatement statement, SQLScript script) {
        // reset all default privileges
        // this generates noisier bit more correct scripts
        // we may have revoked implicit owner GRANT in the previous step, it needs to be restored
        // any privileges in non-default state will be set to their final state in the next step
        // this solution also requires the least amount of handling code: no edge cases
        PgPrivilege.appendDefaultPostgresPrivileges(statement, script);
    }

    @Override
    public void addPrivilege(IPrivilege privilege) {
        String locOwner;
        if (owner == null && getStatementType() == DbObjType.SCHEMA
                && Consts.PUBLIC.equals(getName())) {
            locOwner = "postgres";
        } else {
            locOwner = owner;
        }

        // Skip filtering if statement type is COLUMN, because of the
        // specific relationship with table privileges.
        // The privileges of columns for role are not set lower than for the
        // same role in the parent table, they may be the same or higher.
        if (DbObjType.COLUMN != getStatementType()
                && "ALL".equalsIgnoreCase(privilege.getPermission())) {
            addPrivilegeFiltered(privilege, locOwner);
        } else {
            super.addPrivilege(privilege);
        }
    }

    private void addPrivilegeFiltered(IPrivilege privilege, String locOwner) {
        if ("PUBLIC".equals(privilege.getRole())) {
            boolean isFunc = switch (getStatementType()) {
                case FUNCTION, PROCEDURE, AGGREGATE, DOMAIN, TYPE -> true;
                default -> false;
            };
            if (isFunc != privilege.isRevoke()) {
                return;
            }
        }

        if (!privilege.isRevoke() && privilege.getRole().equals(locOwner)) {
            IPrivilege delRevoke = privileges.stream()
                    .filter(p -> p.isRevoke()
                            && p.getRole().equals(privilege.getRole())
                            && p.getPermission().equals(privilege.getPermission()))
                    .findAny().orElse(null);
            if (delRevoke != null) {
                privileges.remove(delRevoke);
                return;
            }
        }
        privileges.add(privilege);
        resetHash();
    }

    @Override
    public String getRenameCommand(String newName) {
        return RENAME_OBJECT_COMMAND.formatted(getStatementType(), getQualifiedName(), quote(newName));
    }

    /**
     * Wraps SQL in DO block with error handling.
     *
     * @param sbResult        the StringBuilder to append to
     * @param sbSQL           the SQL to wrap
     * @param expectedErrCode the expected error code to handle
     */
    public void appendSqlWrappedInDo(StringBuilder sbResult, StringBuilder sbSQL, String expectedErrCode) {
        String body = sbSQL.toString().replace("\n", "\n\t");

        sbResult
                .append("DO $$")
                .append("\nBEGIN")
                .append("\n\t").append(body)
                .append("\nEXCEPTION WHEN OTHERS THEN")
                .append("\n\tIF (SQLSTATE = ").append(expectedErrCode).append(") THEN")
                .append("\n\t\tRAISE NOTICE '%, skip', SQLERRM;")
                .append("\n\tELSE")
                .append("\n\t\tRAISE;")
                .append("\n\tEND IF;")
                .append("\nEND; $$")
                .append("\nLANGUAGE 'plpgsql'");
    }
}
