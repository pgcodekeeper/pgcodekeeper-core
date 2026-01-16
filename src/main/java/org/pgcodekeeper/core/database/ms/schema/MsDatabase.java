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
package org.pgcodekeeper.core.database.ms.schema;

import java.util.*;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.hasher.Hasher;

/**
 * Represents a Microsoft SQL database with its schemas, assemblies, roles, and users.
 * Provides functionality for managing database-level objects and their relationships.
 */
public final class MsDatabase extends AbstractDatabase implements IMsStatement {

    private final Map<String, MsAssembly> assemblies = new LinkedHashMap<>();
    private final Map<String, MsRole> roles = new LinkedHashMap<>();
    private final Map<String, MsUser> users = new LinkedHashMap<>();

    @Override
    protected void fillChildrenList(List<Collection<? extends AbstractStatement>> l) {
        super.fillChildrenList(l);
        l.add(assemblies.values());
        l.add(roles.values());
        l.add(users.values());
    }

    @Override
    public AbstractStatement getChild(String name, DbObjType type) {
        return switch (type) {
            case SCHEMA -> getSchema(name);
            case ASSEMBLY -> getAssembly(name);
            case ROLE -> getRole(name);
            case USER -> getUser(name);
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
            case ASSEMBLY:
                addAssembly((MsAssembly) st);
                break;
            case ROLE:
                addRole((MsRole) st);
                break;
            case USER:
                addUser((MsUser) st);
                break;
            default:
                throw new IllegalArgumentException("Unsupported child type: " + type);
        }
    }

    /**
     * Returns assembly of given name or null if the assembly has not been found.
     *
     * @param name assembly name
     * @return found assembly or null
     */
    public MsAssembly getAssembly(final String name) {
        return getChildByName(assemblies, name);
    }

    /**
     * Returns role of given name or null if the role has not been found.
     *
     * @param name role name
     * @return found role or null
     */
    public MsRole getRole(final String name) {
        return getChildByName(roles, name);
    }

    /**
     * Returns user of given name or null if the user has not been found.
     *
     * @param name user name
     * @return found user or null
     */
    public MsUser getUser(final String name) {
        return getChildByName(users, name);
    }

    /**
     * Gets all assemblies in this database.
     *
     * @return unmodifiable collection of assemblies
     */
    public Collection<MsAssembly> getAssemblies() {
        return Collections.unmodifiableCollection(assemblies.values());
    }

    /**
     * Gets all roles in this database.
     *
     * @return unmodifiable collection of roles
     */
    public Collection<MsRole> getRoles() {
        return Collections.unmodifiableCollection(roles.values());
    }

    /**
     * Getter for {@link #users}. The list cannot be modified.
     *
     * @return {@link #users}
     */
    public Collection<MsUser> getUsers() {
        return Collections.unmodifiableCollection(users.values());
    }

    /**
     * Adds an assembly to this database.
     *
     * @param assembly the assembly to add
     */
    public void addAssembly(final MsAssembly assembly) {
        addUnique(assemblies, assembly);
    }

    /**
     * Adds a role to this database.
     *
     * @param role the role to add
     */
    public void addRole(final MsRole role) {
        addUnique(roles, role);
    }

    /**
     * Adds a user to this database.
     *
     * @param user the user to add
     */
    public void addUser(final MsUser user) {
        addUnique(users, user);
    }

    @Override
    public boolean compareChildren(AbstractStatement obj) {
        if (obj instanceof MsDatabase db && super.compareChildren(obj)) {
            return assemblies.equals(db.assemblies)
                    && roles.equals(db.roles)
                    && users.equals(db.users);
        }
        return false;
    }

    @Override
    public void computeChildrenHash(Hasher hasher) {
        super.computeChildrenHash(hasher);
        hasher.putUnordered(assemblies);
        hasher.putUnordered(roles);
        hasher.putUnordered(users);
    }

    @Override
    protected boolean isFirstLevelType(DbObjType type) {
        return type.in(DbObjType.SCHEMA, DbObjType.USER, DbObjType.ROLE, DbObjType.ASSEMBLY);
    }

    @Override
    protected AbstractDatabase getDatabaseCopy() {
        return new MsDatabase();
    }

    @Override
    protected AbstractStatement resolveStatistics(AbstractSchema s, GenericColumn gc, DbObjType type) {
        var cont = s.getStatementContainer(gc.table());
        return cont != null ? cont.getChild(gc.column(), type) : null;
    }
}