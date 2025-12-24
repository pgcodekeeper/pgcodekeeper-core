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

import org.pgcodekeeper.core.hasher.IHashable;
import org.pgcodekeeper.core.script.SQLScript;

import java.util.Collection;

/**
 * Represents a database privilege (GRANT/REVOKE) for a database object.
 * Handles privilege operations including creation, dropping, and SQL generation.
 */
public interface IPrivilege extends IHashable {

    String WITH_GRANT_OPTION = " WITH GRANT OPTION";
    String GRANT = "GRANT";
    String REVOKE = "REVOKE";

    /**
     * Checks if this privilege represents a REVOKE operation.
     *
     * @return true if this is a REVOKE privilege, false if GRANT
     */
    boolean isRevoke();

    /**
     * Generates the SQL statement for this privilege.
     *
     * @return the GRANT or REVOKE SQL statement
     */
    String getCreationSQL();

    /**
     * Generates the SQL statement to drop this privilege.
     *
     * @return the REVOKE SQL statement, or null if this is already a REVOKE
     */
    String getDropSQL();

    /**
     * @return the permission type (e.g., SELECT, INSERT, ALL)
     */
    String getPermission();

    /**
     * @return role the role receiving or losing the privilege
     */
    String getRole();

    /**
     * @return the object name the privilege applies to
     */
    String getName();

    /**
     * Appends multiple privileges to a SQL script.
     *
     * @param privileges the collection of privileges to append
     * @param script the script to append to
     */
    static void appendPrivileges(Collection<IPrivilege> privileges, SQLScript script) {
        for (IPrivilege priv : privileges) {
            script.addStatement(priv.getCreationSQL());
        }
    }
}
