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

import org.pgcodekeeper.core.database.base.schema.AbstractPrivilege;

/**
 * Represents a database privilege (GRANT/REVOKE) for ClickHouse database object.
 * Handles privilege operations including creation, dropping, and SQL generation.
 */
public class ChPrivilege extends AbstractPrivilege {

    /**
     * Creates a new privilege instance.
     *
     * @param state the privilege state (GRANT or REVOKE)
     * @param permission the permission type (e.g., SELECT, INSERT, ALL)
     * @param name the object name the privilege applies to
     * @param role the role receiving or losing the privilege
     * @param isGrantOption whether this privilege includes GRANT OPTION
     */
    public ChPrivilege(String state, String permission, String name, String role, boolean isGrantOption) {
        super(state, permission, name, role, isGrantOption);
    }

    @Override
    protected void appendGrandOption(boolean isRevoke, StringBuilder sb) {
        if (!isRevoke) {
            sb.append(WITH_GRANT_OPTION);
        }
    }
}
