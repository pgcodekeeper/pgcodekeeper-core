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
package org.pgcodekeeper.core.database.ch.schema;

import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.database.api.schema.ObjectState;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;

import java.util.Objects;

/**
 * Represents a ClickHouse role for access control.
 * Roles can be stored in different storage types and have associated privileges.
 */
public class ChRole extends ChAbstractStatement {

    private static final String DEF_STORAGE = "local_directory";

    private String storageType = DEF_STORAGE;

    /**
     * Creates a new ClickHouse role with the specified name.
     *
     * @param name the name of the role
     */
    public ChRole(String name) {
        super(name);
    }

    @Override
    public DbObjType getStatementType() {
        return DbObjType.ROLE;
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        final StringBuilder sbSQL = new StringBuilder();
        sbSQL.append("CREATE ROLE ");
        appendIfNotExists(sbSQL, script.getSettings());
        sbSQL.append(getQuotedName());
        if (!DEF_STORAGE.equals(storageType)) {
            sbSQL.append("\n\tIN ").append(storageType);
        }
        script.addStatement(sbSQL);
        appendPrivileges(script);
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        int startSize = script.getSize();
        ChRole newRole = (ChRole) newCondition;

        if (!Objects.equals(storageType, newRole.storageType)) {
            StringBuilder sql = new StringBuilder();
            sql.append("MOVE ROLE ")
                    .append(getQualifiedName()).append(" TO ")
                    .append(newRole.storageType);
            script.addStatement(sql);
        }
        alterPrivileges(newRole, script);
        return getObjectState(script, startSize);
    }

    public void setStorageType(String storageType) {
        this.storageType = storageType;
        resetHash();
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.put(storageType);
    }

    @Override
    public boolean compare(IStatement obj) {
        if (this == obj) {
            return true;
        }
        return obj instanceof ChRole role
                && super.compare(role)
                && Objects.equals(storageType, role.storageType);
    }

    @Override
    protected AbstractStatement getCopy() {
        ChRole copy = new ChRole(name);
        copy.setStorageType(storageType);
        return copy;
    }
}