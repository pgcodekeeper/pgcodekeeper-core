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
package org.pgcodekeeper.core.database.base.schema;

import org.pgcodekeeper.core.database.api.schema.ObjectPrivilege;

import java.util.ArrayList;
import java.util.List;

/**
 * Contains override information for database statements including owner and privileges.
 * Used when applying custom ownership and privilege settings to database objects.
 */
public class StatementOverride {
    private String owner;
    private final List<ObjectPrivilege> privileges = new ArrayList<>();

    /**
     * Adds a privilege override to this statement.
     *
     * @param privilege the privilege to add
     */
    public void addPrivilege(ObjectPrivilege privilege) {
        privileges.add(privilege);
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getOwner() {
        return owner;
    }

    public List<ObjectPrivilege> getPrivileges() {
        return privileges;
    }
}
