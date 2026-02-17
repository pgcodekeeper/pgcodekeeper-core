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

import org.pgcodekeeper.core.database.api.jdbc.ISupportedVersion;
import org.pgcodekeeper.core.database.api.launcher.IAnalysisLauncher;
import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.database.ms.jdbc.MsSupportedVersion;
import org.pgcodekeeper.core.hasher.Hasher;

/**
 * Represents a Microsoft SQL database with its schemas, assemblies, roles, and users.
 * Provides functionality for managing database-level objects and their relationships.
 */
public class MsDatabase extends MsAbstractStatement implements IDatabase {

    private final Map<String, MsSchema> schemas = new LinkedHashMap<>();
    private final List<ObjectOverride> overrides = new ArrayList<>();
    // Contains object references
    private final Map<String, Set<ObjectLocation>> objReferences = new HashMap<>();
    // Contains analysis launchers for all statements
    // (used for launch analyze and getting dependencies).
    private final ArrayList<IAnalysisLauncher> analysisLaunchers = new ArrayList<>();
    private final Map<String, MsAssembly> assemblies = new LinkedHashMap<>();
    private final Map<String, MsRole> roles = new LinkedHashMap<>();
    private final Map<String, MsUser> users = new LinkedHashMap<>();

    private MsSupportedVersion version = MsSupportedVersion.VERSION_17;
    /**
     * Current default schema.
     */
    private MsSchema defaultSchema;

    public MsDatabase() {
        super("DB_name_placeholder");
    }

    @Override
    public IStatement getStatement(ObjectReference reference) {
        DbObjType type = reference.type();
        if (type == DbObjType.DATABASE) {
            return this;
        }

        if (type.in(DbObjType.SCHEMA, DbObjType.USER, DbObjType.ROLE, DbObjType.ASSEMBLY)) {
            return getChild(reference.schema(), type);
        }

        MsSchema s = getSchema(reference.schema());
        if (s == null) {
            return null;
        }

        return switch (type) {
            case SEQUENCE, VIEW -> s.getChild(reference.table(), type);
            case TYPE -> resolveTypeCall(s, reference.table());
            case FUNCTION, PROCEDURE -> resolveFunctionCall(s, reference.table());
            case TABLE -> s.getRelation(reference.table());
            case INDEX -> s.getIndexByName(reference.table());
            // handled in getStatement, left here for consistency
            case COLUMN -> {
                MsTable t = s.getTable(reference.table());
                yield t == null ? null : t.getColumn(reference.column());
            }
            case STATISTICS, CONSTRAINT, TRIGGER -> {
                var sc = s.getStatementContainer(reference.table());
                yield sc == null ? null : sc.getChild(reference.column(), type);
            }
            default -> throw new IllegalStateException("Unhandled DbObjType: " + type);
        };
    }

    private IStatement resolveTypeCall(MsSchema s, String table) {
        IStatement st = s.getChild(table, DbObjType.TYPE);
        if (st != null) {
            return st;
        }
        // every "selectable" relation can be used as a type
        // getRelation should only look for "selectable" relations
        return s.getRelation(table);
    }

    private IFunction resolveFunctionCall(MsSchema schema, String table) {
        for (IFunction f : schema.getFunctions()) {
            if (f.getBareName().equals(table)) {
                return f;
            }
        }
        return null;
    }

    /**
     * Clears all analysis launchers and trims the internal list to size.
     */
    @Override
    public void clearAnalysisLaunchers() {
        analysisLaunchers.clear();
        analysisLaunchers.trimToSize();
    }

    @Override
    public void fillDescendantsList(List<Collection<? extends AbstractStatement>> l) {
        fillChildrenList(l);
        for (var schema : schemas.values()) {
            schema.fillDescendantsList(l);
        }
    }

    @Override
    public void fillChildrenList(List<Collection<? extends AbstractStatement>> l) {
        l.add(schemas.values());
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
    public Collection<IStatement> getChildrenByType(DbObjType type) {
        return switch (type) {
            case SCHEMA -> Collections.unmodifiableCollection(schemas.values());
            case ASSEMBLY -> Collections.unmodifiableCollection(assemblies.values());
            case ROLE -> Collections.unmodifiableCollection(roles.values());
            case USER -> Collections.unmodifiableCollection(users.values());
            default -> List.of();
        };
    }

    @Override
    public void addChild(IStatement st) {
        DbObjType type = st.getStatementType();
        switch (type) {
            case SCHEMA:
                addSchema((MsSchema) st);
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

    /**
     * Getter for {@link #schemas}. The list cannot be modified.
     *
     * @return {@link #schemas}
     */
    @Override
    public Collection<MsSchema> getSchemas() {
        return Collections.unmodifiableCollection(schemas.values());
    }

    public void setVersion(MsSupportedVersion version) {
        this.version = version;
    }

    @Override
    public ISupportedVersion getVersion() {
        return version;
    }

    @Override
    public MsSchema getDefaultSchema() {
        return defaultSchema;
    }

    @Override
    public void setDefaultSchema(String defaultSchemaName) {
        this.defaultSchema = getSchema(defaultSchemaName);
    }

    @Override
    public void addOverride(ObjectOverride override) {
        overrides.add(override);
    }

    @Override
    public Collection<ObjectOverride> getOverrides() {
        return overrides;
    }

    @Override
    public Map<String, Set<ObjectLocation>> getObjReferences() {
        return objReferences;
    }

    @Override
    public List<IAnalysisLauncher> getAnalysisLaunchers() {
        return analysisLaunchers;
    }

    /**
     * Add 'analysis launcher' for deferred analyze.
     *
     * @param launcher launcher that contains almost everything needed to analyze a statement contained in it
     */
    @Override
    public void addAnalysisLauncher(IAnalysisLauncher launcher) {
        analysisLaunchers.add(launcher);
    }

    @Override
    public void addReference(String fileName, ObjectLocation loc) {
        objReferences.computeIfAbsent(fileName, k -> new LinkedHashSet<>()).add(loc);
    }

    /**
     * Returns schema of given name or null if the schema has not been found. If schema name is null then default schema
     * is returned.
     *
     * @param name schema name or null which means default schema
     * @return found schema or null
     */
    @Override
    public MsSchema getSchema(final String name) {
        if (name == null) {
            return getDefaultSchema();
        }

        return getChildByName(schemas, name);
    }

    /**
     * Adds a schema to this database.
     *
     * @param schema the schema to add
     */
    public void addSchema(final MsSchema schema) {
        addUnique(schemas, schema);
    }

    @Override
    public void computeHash(Hasher hasher) {
        // has only child objects
    }

    @Override
    public void computeChildrenHash(Hasher hasher) {
        hasher.putUnordered(assemblies);
        hasher.putUnordered(roles);
        hasher.putUnordered(users);
    }

    @Override
    public boolean compareChildren(AbstractStatement obj) {
        if (this == obj) {
            return true;
        }
        return obj instanceof MsDatabase db
                && super.compareChildren(db)
                && assemblies.equals(db.assemblies)
                && roles.equals(db.roles)
                && users.equals(db.users);
    }


    @Override
    protected MsDatabase getCopy() {
        MsDatabase dbDst = new MsDatabase();
        dbDst.setVersion(version);
        return dbDst;
    }
}