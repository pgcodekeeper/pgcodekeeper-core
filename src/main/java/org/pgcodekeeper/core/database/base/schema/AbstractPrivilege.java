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
package org.pgcodekeeper.core.database.base.schema;

import org.pgcodekeeper.core.database.api.schema.IPrivilege;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.hasher.JavaHasher;

import java.util.Objects;

/**
 * Represents a database privilege (GRANT/REVOKE) for a database object.
 * Handles privilege operations including creation, dropping, and SQL generation
 * for different database types.
 */
public abstract class AbstractPrivilege implements IPrivilege {

    private final String state;
    private final String permission;
    private final String role;
    private final String name;
    private final boolean isGrantOption;

    /**
     * Creates a new privilege instance.
     *
     * @param state the privilege state (GRANT or REVOKE)
     * @param permission the permission type (e.g., SELECT, INSERT, ALL)
     * @param name the object name the privilege applies to
     * @param role the role receiving or losing the privilege
     * @param isGrantOption whether this privilege includes GRANT OPTION
     */
    protected AbstractPrivilege(String state, String permission, String name, String role, boolean isGrantOption) {
        this.state = state;
        this.permission = permission;
        this.name = name;
        this.role = role;
        this.isGrantOption = isGrantOption;
    }

    public boolean isRevoke() {
        return REVOKE.equalsIgnoreCase(state);
    }

    public String getCreationSQL() {
        return getSql(isRevoke());
    }

    protected String getSql(boolean isRevoke) {
        StringBuilder sb = new StringBuilder();
        sb.append(isRevoke ? REVOKE : GRANT).append(' ').append(permission);
        if (name != null) {
            sb.append(" ON ").append(name);
        }

        sb.append(isRevoke ? " FROM " : " TO ").append(role);

        if (isGrantOption) {
            appendGrandOption(isRevoke, sb);
        }

        return sb.toString();
    }

    protected void appendGrandOption(boolean isRevoke, StringBuilder sb) {
        sb.append(isRevoke ? " CASCADE" : WITH_GRANT_OPTION);
    }

    @Override
    public String getDropSQL() {
        return isRevoke() ? null : getSql(true);
    }

    @Override
    public String getPermission() {
        return permission;
    }

    @Override
    public String getRole() {
        return role;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        return obj instanceof AbstractPrivilege priv
                && isGrantOption == priv.isGrantOption
                && Objects.equals(state, priv.state)
                && Objects.equals(permission, priv.permission)
                && Objects.equals(role, priv.role)
                && Objects.equals(name, priv.name);
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
}
