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

import java.util.*;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.hasher.Hasher;

/**
 * Abstract base class for database row-level security policies.
 * Provides common functionality for policies that control access to table rows
 * based on events, roles, and conditions.
 */
public abstract class AbstractPolicy extends AbstractStatement {

    protected EventType event;
    protected final Set<String> roles = new LinkedHashSet<>();
    protected String using;
    protected boolean isPermissive = true;

    protected AbstractPolicy(String name) {
        super(name);
    }

    @Override
    public DbObjType getStatementType() {
        return DbObjType.POLICY;
    }

    public void setEvent(EventType event) {
        this.event = event;
        resetHash();
    }

    /**
     * Adds a role to this policy.
     *
     * @param role the role name to add
     */
    public void addRole(String role) {
        roles.add(role);
        resetHash();
    }

    public void setUsing(String using) {
        this.using = using;
        resetHash();
    }

    public void setPermissive(boolean isPermissive) {
        this.isPermissive = isPermissive;
        resetHash();
    }

    @Override
    public AbstractPolicy shallowCopy() {
        AbstractPolicy copy = getPolicyCopy();
        copyBaseFields(copy);
        copy.setPermissive(isPermissive);
        copy.setEvent(event);
        copy.roles.addAll(roles);
        copy.setUsing(using);
        return copy;
    }

    protected abstract AbstractPolicy getPolicyCopy();

    @Override
    public void computeHash(Hasher hasher) {
        hasher.put(isPermissive);
        hasher.put(event);
        hasher.put(roles);
        hasher.put(using);
    }

    @Override
    public boolean compare(IStatement obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof AbstractPolicy police && super.compare(obj)) {
            return isPermissive == police.isPermissive
                    && event == police.event
                    && roles.equals(police.roles)
                    && Objects.equals(using, police.using);
        }

        return false;
    }
}
