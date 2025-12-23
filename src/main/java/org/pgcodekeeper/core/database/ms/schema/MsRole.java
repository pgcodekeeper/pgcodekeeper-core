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

import org.pgcodekeeper.core.database.api.schema.DatabaseType;
import org.pgcodekeeper.core.MsDiffUtils;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.base.schema.AbstractDatabase;
import org.pgcodekeeper.core.database.api.schema.ObjectState;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.script.SQLScript;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a Microsoft SQL database role.
 * Roles are used to group users and manage permissions at the database level.
 */
public final class MsRole extends AbstractStatement implements IMsStatement {

    private final Set<String> members = new LinkedHashSet<>();

    /**
     * Creates a new Microsoft SQL role.
     *
     * @param name the role name
     */
    public MsRole(String name) {
        super(name);
    }

    @Override
    public DbObjType getStatementType() {
        return DbObjType.ROLE;
    }

    @Override
    public AbstractDatabase getDatabase() {
        return (AbstractDatabase) parent;
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        final StringBuilder sbSQL = new StringBuilder();
        sbSQL.append("CREATE ROLE ");
        sbSQL.append(MsDiffUtils.quoteName(name));
        if (owner != null) {
            sbSQL.append("\nAUTHORIZATION ").append(MsDiffUtils.quoteName(owner));
        }
        script.addStatement(sbSQL);

        for (String member : members) {
            appendAlterRole(member, script, true);
        }

        appendPrivileges(script);
    }

    @Override
    public void getDropSQL(SQLScript script, boolean optionExists) {
        for (String member : members) {
            appendAlterRole(member, script, false);
        }
        super.getDropSQL(script, optionExists);
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        int startSize = script.getSize();
        MsRole newRole = (MsRole) newCondition;

        appendAlterOwner(newRole, script);

        if (!Objects.equals(members, newRole.members)) {
            for (String newMember : newRole.members) {
                if (!members.contains(newMember)) {
                    appendAlterRole(newMember, script, true);
                }
            }

            for (String oldMember : members) {
                if (!newRole.members.contains(oldMember)) {
                    appendAlterRole(oldMember, script, false);
                }
            }
        }

        alterPrivileges(newRole, script);

        return getObjectState(script, startSize);
    }

    private void appendAlterRole(String member, SQLScript script, boolean needAddMember) {
        StringBuilder sql = new StringBuilder();
        sql.append("ALTER ROLE ").append(MsDiffUtils.quoteName(name));
        sql.append(needAddMember ? " ADD " : " DROP ").append("MEMBER ").append(MsDiffUtils.quoteName(member));
        script.addStatement(sql);
    }

    /**
     * Adds a member to this role.
     *
     * @param member the user or role name to add as a member
     */
    public void addMember(String member) {
        members.add(member);
        resetHash();
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.put(members);
    }

    @Override
    public MsRole shallowCopy() {
        MsRole roleDst = new MsRole(name);
        copyBaseFields(roleDst);
        roleDst.members.addAll(members);
        return roleDst;
    }

    @Override
    public boolean compare(IStatement obj) {
        if (obj == this) {
            return true;
        }

        return obj instanceof MsRole role && super.compare(obj)
                && Objects.equals(members, role.members);
    }

    @Override
    public DatabaseType getDbType() {
        return DatabaseType.MS;
    }
}
