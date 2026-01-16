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

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.hasher.Hasher;

/**
 * Represents a ClickHouse database with its schema objects.
 * Contains collections of ClickHouse-specific objects like functions, policies, users, and roles
 * in addition to the standard schemas.
 */
public final class ChDatabase extends AbstractDatabase implements IChStatement {

    private final Map<String, ChFunction> functions = new LinkedHashMap<>();
    private final Map<String, ChPolicy> policies = new LinkedHashMap<>();
    private final Map<String, ChUser> users = new LinkedHashMap<>();
    private final Map<String, ChRole> roles = new LinkedHashMap<>();

    @Override
    protected void fillChildrenList(List<Collection<? extends AbstractStatement>> l) {
        super.fillChildrenList(l);
        l.add(functions.values());
        l.add(policies.values());
        l.add(users.values());
        l.add(roles.values());
    }

    @Override
    public AbstractStatement getChild(String name, DbObjType type) {
        return switch (type) {
            case SCHEMA -> getSchema(name);
            case FUNCTION -> getChildByName(functions, name);
            case POLICY -> getChildByName(policies, name);
            case USER -> getChildByName(users, name);
            case ROLE -> getChildByName(roles, name);
            default -> null;
        };
    }

    @Override
    public void addChild(IStatement st) {
        DbObjType type = st.getStatementType();
        switch (type) {
            case SCHEMA:
                addSchema((AbstractSchema) st);
                break;
            case FUNCTION:
                addFunction((ChFunction) st);
                break;
            case POLICY:
                addPolicy((ChPolicy) st);
                break;
            case USER:
                addUser((ChUser) st);
                break;
            case ROLE:
                addRole((ChRole) st);
                break;
            default:
                throw new IllegalArgumentException("Unsupported child type: " + type);
        }
    }

    private void addFunction(final ChFunction function) {
        addUnique(functions, function);
    }

    private void addPolicy(final ChPolicy policy) {
        addUnique(policies, policy);
    }

    private void addUser(final ChUser user) {
        addUnique(users, user);
    }

    private void addRole(final ChRole role) {
        addUnique(roles, role);
    }

    @Override
    public boolean compareChildren(AbstractStatement obj) {
        if (obj instanceof ChDatabase db && super.compareChildren(obj)) {
            return functions.equals(db.functions)
                    && users.equals(db.users)
                    && roles.equals(db.roles)
                    && policies.equals(db.policies);
        }
        return false;
    }

    @Override
    public void computeChildrenHash(Hasher hasher) {
        super.computeChildrenHash(hasher);
        hasher.putUnordered(functions);
        hasher.putUnordered(users);
        hasher.putUnordered(roles);
        hasher.putUnordered(policies);
    }

    @Override
    protected boolean isFirstLevelType(DbObjType type) {
        return type.in(DbObjType.SCHEMA, DbObjType.POLICY, DbObjType.FUNCTION, DbObjType.USER, DbObjType.ROLE);
    }

    @Override
    protected AbstractDatabase getDatabaseCopy() {
        return new ChDatabase();
    }
}
