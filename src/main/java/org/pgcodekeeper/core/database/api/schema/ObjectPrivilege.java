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
package org.pgcodekeeper.core.database.api.schema;

import org.pgcodekeeper.core.PgDiffUtils;
import org.pgcodekeeper.core.database.pg.schema.PgAbstractFunction;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.hasher.IHashable;
import org.pgcodekeeper.core.hasher.JavaHasher;
import org.pgcodekeeper.core.script.SQLScript;

import java.util.Collection;
import java.util.Objects;

/**
 * Represents a database privilege (GRANT/REVOKE) for a database object.
 * Handles privilege operations including creation, dropping, and SQL generation
 * for different database types.
 */
public final class ObjectPrivilege implements IHashable {

    // FIXME move DB-specific logic into their packages

    private static final String WITH_GRANT_OPTION = " WITH GRANT OPTION";
    private static final String GRANT = "GRANT";
    private static final String REVOKE = "REVOKE";

    private final String state;
    private final String permission;
    private final String role;
    private final String name;
    private final boolean isGrantOption;
    private final DatabaseType dbType;

    /**
     * Checks if this privilege represents a REVOKE operation.
     *
     * @return true if this is a REVOKE privilege, false if GRANT
     */
    public boolean isRevoke() {
        return REVOKE.equalsIgnoreCase(state);
    }

    /**
     * Creates a new privilege instance.
     *
     * @param state the privilege state (GRANT or REVOKE)
     * @param permission the permission type (e.g., SELECT, INSERT, ALL)
     * @param name the object name the privilege applies to
     * @param role the role receiving or losing the privilege
     * @param isGrantOption whether this privilege includes GRANT OPTION
     * @param dbType the database type this privilege applies to
     */
    public ObjectPrivilege(String state, String permission, String name, String role, boolean isGrantOption, DatabaseType dbType) {
        this.state = state;
        this.permission = permission;
        this.name = name;
        this.role = role;
        this.isGrantOption = isGrantOption;
        this.dbType = dbType;
    }

    /**
     * Generates the SQL statement for this privilege.
     *
     * @return the GRANT or REVOKE SQL statement
     */
    public String getCreationSQL() {
        StringBuilder sb = new StringBuilder();
        sb.append(state).append(' ').append(permission);
        if (name != null) {
            sb.append(" ON ").append(name);
        }

        sb.append(isRevoke() ? " FROM " : " TO ").append(role);

        if (isGrantOption) {
            String cascade = dbType == DatabaseType.CH ? "" : " CASCADE";
            sb.append(isRevoke() ? cascade : WITH_GRANT_OPTION);
        }

        return sb.toString();
    }

    /**
     * Generates the SQL statement to drop this privilege.
     *
     * @return the REVOKE SQL statement, or null if this is already a REVOKE
     */
    public String getDropSQL() {
        if (isRevoke()) {
            return null;
        }

        return new ObjectPrivilege(REVOKE, permission, name, role, isGrantOption, dbType).getCreationSQL();
    }

    /**
     * Appends multiple privileges to a SQL script.
     *
     * @param privileges the collection of privileges to append
     * @param script the script to append to
     */
    public static void appendPrivileges(Collection<ObjectPrivilege> privileges, SQLScript script) {
        for (ObjectPrivilege priv : privileges) {
            script.addStatement(priv.getCreationSQL());
        }
    }

    /**
     * Appends default PostgreSQL privileges for a database object.
     *
     * @param newObj the database object to set default privileges for
     * @param script the script to append privileges to
     */
    public static void appendDefaultPostgresPrivileges(IStatement newObj, SQLScript script) {
        DbObjType type = newObj.getStatementType();
        boolean isFunctionOrTypeOrDomain = false;
        String typeName;
        switch (type) {
            case FUNCTION:
            case PROCEDURE:
            case TYPE:
            case DOMAIN:
                isFunctionOrTypeOrDomain = true;
                typeName = type.name();
                break;
            case AGGREGATE:
                isFunctionOrTypeOrDomain = true;
                typeName = DbObjType.FUNCTION.name();
                break;
            case FOREIGN_DATA_WRAPPER:
                typeName = "FOREIGN DATA WRAPPER";
                break;
            case SERVER:
                typeName = "FOREIGN SERVER";
                break;
            case VIEW:
                typeName = DbObjType.TABLE.name();
                break;
            case SCHEMA:
            case SEQUENCE:
            case TABLE:
                typeName = type.name();
                break;
            default:
                return;
        }

        StringBuilder sbName = new StringBuilder()
                .append(typeName)
                .append(' ');
        if (newObj instanceof PgAbstractFunction func) {
            sbName.append(PgDiffUtils.getQuotedName(func.getParent().getName())).append('.');
            func.appendFunctionSignature(sbName, false, true);
        } else {
            sbName.append(newObj.getQualifiedName());
        }
        String name = sbName.toString();

        // FUNCTION/PROCEDURE/AGGREGATE/TYPE/DOMAIN by default has "GRANT ALL to PUBLIC".
        // That's why for them set "GRANT ALL to PUBLIC".
        ObjectPrivilege priv = new ObjectPrivilege(isFunctionOrTypeOrDomain ? GRANT : REVOKE,
                "ALL", name, "PUBLIC", false, DatabaseType.PG);
        script.addStatement(priv.getCreationSQL());

        String owner = newObj.getOwner();
        if (owner == null) {
            return;
        }
        owner = PgDiffUtils.getQuotedName(owner);

        addDefPostgresPrivileges(script, REVOKE, name, owner);
        addDefPostgresPrivileges(script, GRANT, name, owner);
    }

    private static void addDefPostgresPrivileges(SQLScript script, String state, String name, String owner) {
        var priv = new ObjectPrivilege(state, "ALL", name, owner, false, DatabaseType.PG);
        script.addStatement(priv.getCreationSQL());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof ObjectPrivilege priv) {
            return isGrantOption == priv.isGrantOption
                    && Objects.equals(state, priv.state)
                    && Objects.equals(permission, priv.permission)
                    && Objects.equals(role, priv.role)
                    && Objects.equals(name, priv.name);
        }

        return false;
    }

    @Override
    public int hashCode() {
        JavaHasher hasher = new JavaHasher();
        computeHash(hasher);
        return hasher.getResult();
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.put(state);
        hasher.put(permission);
        hasher.put(role);
        hasher.put(name);
        hasher.put(isGrantOption);
    }

    @Override
    public String toString() {
        return getCreationSQL();
    }

    public String getPermission() {
        return permission;
    }

    public String getRole() {
        return role;
    }

    public String getName() {
        return name;
    }
}
