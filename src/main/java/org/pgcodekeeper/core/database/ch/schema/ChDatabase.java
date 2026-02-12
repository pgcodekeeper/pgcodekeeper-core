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

import org.pgcodekeeper.core.database.api.launcher.IAnalysisLauncher;
import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.database.ch.jdbc.ChSupportedVersion;
import org.pgcodekeeper.core.hasher.Hasher;

import java.util.*;

/**
 * Represents a ClickHouse database with its schema objects. Contains
 * collections of ClickHouse-specific objects like functions, policies, users,
 * and roles in addition to the standard schemas.
 */
public class ChDatabase extends ChAbstractStatement implements IDatabase {

    private ChSupportedVersion version = ChSupportedVersion.DEFAULT;

    private final List<ObjectOverride> overrides = new ArrayList<>();

    // Contains object references
    private final Map<String, Set<ObjectLocation>> objReferences = new HashMap<>();
    // Contains analysis launchers for all statements
    // (used for launch analyze and getting dependencies).
    private final ArrayList<IAnalysisLauncher> analysisLaunchers = new ArrayList<>();

    /**
     * Current default schema.
     */
    private ChSchema defaultSchema;
    private final Map<String, ChSchema> schemas = new LinkedHashMap<>();

    private final Map<String, ChFunction> functions = new LinkedHashMap<>();
    private final Map<String, ChPolicy> policies = new LinkedHashMap<>();
    private final Map<String, ChUser> users = new LinkedHashMap<>();
    private final Map<String, ChRole> roles = new LinkedHashMap<>();

    public ChDatabase() {
        super("DB_name_placeholder");
    }

    @Override
    public void fillChildrenList(List<Collection<? extends AbstractStatement>> l) {
        l.add(schemas.values());
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
    public Collection<IStatement> getChildrenByType(DbObjType type) {
        return switch (type) {
        case SCHEMA -> Collections.unmodifiableCollection(schemas.values());
        case FUNCTION -> Collections.unmodifiableCollection(functions.values());
        case POLICY -> Collections.unmodifiableCollection(policies.values());
        case USER -> Collections.unmodifiableCollection(users.values());
        case ROLE -> Collections.unmodifiableCollection(roles.values());
        default -> List.of();
        };
    }

    @Override
    public void addChild(IStatement st) {
        DbObjType type = st.getStatementType();
        switch (type) {
        case SCHEMA:
            addUnique(schemas, (ChSchema) st);
            break;
        case FUNCTION:
            addUnique(functions, (ChFunction) st);
            break;
        case POLICY:
            addUnique(policies, (ChPolicy) st);
            break;
        case USER:
            addUnique(users, (ChUser) st);
            break;
        case ROLE:
            addUnique(roles, (ChRole) st);
            break;
        default:
            throw new IllegalArgumentException("Unsupported child type: " + type);
        }
    }

    @Override
    public List<ObjectOverride> getOverrides() {
        return overrides;
    }

    @Override
    public ChSupportedVersion getVersion() {
        return version;
    }

    public void setVersion(ChSupportedVersion version) {
        this.version = version;
    }

    @Override
    public Map<String, Set<ObjectLocation>> getObjReferences() {
        return objReferences;
    }

    @Override
    public void addReference(String fileName, ObjectLocation loc) {
        objReferences.computeIfAbsent(fileName, k -> new LinkedHashSet<>()).add(loc);
    }

    @Override
    public List<IAnalysisLauncher> getAnalysisLaunchers() {
        return analysisLaunchers;
    }

    @Override
    public void clearAnalysisLaunchers() {
        analysisLaunchers.clear();
        analysisLaunchers.trimToSize();
    }

    @Override
    public void addAnalysisLauncher(IAnalysisLauncher launcher) {
        analysisLaunchers.add(launcher);
    }

    @Override
    public void fillDescendantsList(List<Collection<? extends AbstractStatement>> l) {
        fillChildrenList(l);
        for (var schema : schemas.values()) {
            schema.fillDescendantsList(l);
        }
    }

    @Override
    public void setDefaultSchema(final String name) {
        defaultSchema = getChildByName(schemas, name);
    }

    @Override
    public ChSchema getDefaultSchema() {
        return defaultSchema;
    }

    @Override
    public Collection<? extends ISchema> getSchemas() {
        return Collections.unmodifiableCollection(schemas.values());
    }

    @Override
    public ChSchema getSchema(final String name) {
        if (name == null) {
            return getDefaultSchema();
        }

        return getChildByName(schemas, name);
    }

    @Override
    public void addOverride(ObjectOverride override) {
        overrides.add(override);
    }

    /**
     * Copies analysis launchers from another database to this one.
     *
     * @param db the database to copy launchers from
     */
    public void copyLaunchers(ChDatabase db) {
        analysisLaunchers.addAll(db.analysisLaunchers);
    }

    @Override
    public void computeChildrenHash(Hasher hasher) {
        hasher.putUnordered(functions);
        hasher.putUnordered(schemas);
        hasher.putUnordered(users);
        hasher.putUnordered(roles);
        hasher.putUnordered(policies);
    }

    @Override
    public boolean compareChildren(AbstractStatement obj) {
        if (obj instanceof ChDatabase db && super.compareChildren(obj)) {
            return functions.equals(db.functions)
                    && schemas.equals(db.schemas)
                    && users.equals(db.users)
                    && roles.equals(db.roles)
                    && policies.equals(db.policies);
        }
        return false;
    }

    @Override
    public void computeHash(Hasher hasher) {
        // has only child objects
    }

    @Override
    protected ChDatabase getCopy() {
        ChDatabase dbDst = new ChDatabase();
        dbDst.setVersion(version);
        return dbDst;
    }

    /**
     * Resolves and returns a database statement based on the provided generic
     * column specification.
     *
     * @param gc the generic column specification containing type and naming
     *           information
     * @return the resolved statement, or null if not found
     */
    @Override
    public final IStatement getStatement(GenericColumn gc) {
        DbObjType type = gc.type();
        if (type == DbObjType.DATABASE) {
            return this;
        }

        if (type.in(DbObjType.SCHEMA, DbObjType.POLICY, DbObjType.FUNCTION, DbObjType.USER, DbObjType.ROLE)) {
            return getChild(gc.schema(), type);
        }

        ChSchema s = getSchema(gc.schema());
        if (s == null) {
            return null;
        }

        return switch (type) {
        case VIEW, TABLE, DICTIONARY -> s.getRelation(gc.table());
        case FUNCTION -> resolveFunctionCall(gc.table());
        case INDEX -> s.getIndexByName(gc.table());
        // handled in getStatement, left here for consistency
        case COLUMN -> {
            var t = s.getTable(gc.table());
            yield t == null ? null : t.getColumn(gc.column());
        }
        case CONSTRAINT -> {
            var sc = s.getStatementContainer(gc.table());
            yield sc == null ? null : sc.getChild(gc.column(), type);
        }
        default -> throw new IllegalStateException("Unhandled DbObjType: " + type);
        };
    }

    private AbstractStatement resolveFunctionCall(String table) {
        for (var f : functions.values()) {
            if (f.getBareName().equals(table)) {
                return f;
            }
        }
        return null;
    }
}
