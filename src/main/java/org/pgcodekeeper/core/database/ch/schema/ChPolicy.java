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

import java.util.*;
import java.util.function.UnaryOperator;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;

/**
 * Represents a ClickHouse row-level security policy.
 * Supports role-based access control with EXCEPT clauses for role exclusions.
 */
public class ChPolicy extends ChAbstractStatement implements IPolicy {

    protected EventType event;
    protected final Set<String> roles = new LinkedHashSet<>();
    protected String using;
    protected boolean isPermissive = true;
    private final Set<String> excepts = new LinkedHashSet<>();

    /**
     * Creates a new ClickHouse policy with the specified name.
     *
     * @param name the name of the policy
     */
    public ChPolicy(String name) {
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

    /**
     * Adds a role to the EXCEPT list for this policy.
     *
     * @param except the role name to exclude
     */
    public void addExcept(String except) {
        this.excepts.add(except);
        resetHash();
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        appendFullSQL(script, true);
    }

    private void appendFullSQL(SQLScript script, boolean isCreate) {
        final StringBuilder sbSQL = new StringBuilder();

        sbSQL.append(isCreate ? "CREATE" : "ALTER").append(" POLICY ");

        if (isCreate) {
            appendIfNotExists(sbSQL, script.getSettings());
        }

        sbSQL.append(name);

        if (event != null) {
            sbSQL.append("\n  FOR ").append(event);
        }

        if (using != null && !using.isEmpty()) {
            sbSQL.append("\n  USING ").append(using);
        }

        if (!isPermissive) {
            sbSQL.append("\n  AS RESTRICTIVE");
        }

        if (!roles.isEmpty()) {
            sbSQL.append("\n  TO ");
            sbSQL.append(String.join(", ", roles));
        } else if (!excepts.isEmpty() || !isCreate) {
            sbSQL.append("\n  TO ALL");
        }

        if (!excepts.isEmpty()) {
            sbSQL.append(" EXCEPT ").append(String.join(", ", excepts));
        }
        script.addStatement(sbSQL);
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        int startSize = script.getSize();
        ChPolicy police = (ChPolicy) newCondition;

        if (!compare(police)) {
            police.appendFullSQL(script, false);
        }

        return getObjectState(script, startSize);
    }

    @Override
    protected ChPolicy getCopy() {
        ChPolicy copy = new ChPolicy(name);
        copy.setPermissive(isPermissive);
        copy.setEvent(event);
        copy.setUsing(using);
        copy.roles.addAll(roles);
        copy.excepts.addAll(excepts);
        return copy;
    }


    @Override
    public void computeHash(Hasher hasher) {
        hasher.put(isPermissive);
        hasher.put(event);
        hasher.put(using);
        hasher.put(roles);
        hasher.put(excepts);
    }

    @Override
    public boolean compare(IStatement obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof ChPolicy police && super.compare(obj)) {
            return isPermissive == police.isPermissive
                    && event == police.event
                    && Objects.equals(using, police.using)
                    && roles.equals(police.roles)
                    && excepts.equals(police.excepts);
        }

        return false;
    }

    @Override
    public UnaryOperator<String> getQuoter() {
        return (name) -> name;
    }
}
