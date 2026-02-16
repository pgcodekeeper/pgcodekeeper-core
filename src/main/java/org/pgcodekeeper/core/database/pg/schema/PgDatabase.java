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
 **
 * Copyright 2006 StartNet s.r.o.
 *
 * Distributed under MIT license
 *******************************************************************************/
package org.pgcodekeeper.core.database.pg.schema;

import java.util.*;

import org.pgcodekeeper.core.database.api.jdbc.ISupportedVersion;
import org.pgcodekeeper.core.database.api.launcher.IAnalysisLauncher;
import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.database.pg.jdbc.PgSupportedVersion;
import org.pgcodekeeper.core.database.pg.utils.PgConsts;
import org.pgcodekeeper.core.hasher.Hasher;

/**
 * PostgreSQL database implementation.
 * Manages database-level objects such as schemas, extensions, event triggers,
 * foreign data wrappers, servers, user mappings, and casts.
 *
 * @author fordfrog
 */
public final class PgDatabase extends PgAbstractStatement implements IDatabase {

    private PgSchema defaultSchema;
    private PgSupportedVersion version = PgSupportedVersion.VERSION_14;

    private final List<ObjectOverride> overrides = new ArrayList<>();

    // Contains object references
    private final Map<String, Set<ObjectLocation>> objReferences = new HashMap<>();
    // Contains analysis launchers for all statements
    // (used for launch analyze and getting dependencies).
    private final ArrayList<IAnalysisLauncher> analysisLaunchers = new ArrayList<>();

    private final Map<String, PgExtension> extensions = new LinkedHashMap<>();
    private final Map<String, PgEventTrigger> eventTriggers = new LinkedHashMap<>();
    private final Map<String, PgForeignDataWrapper> fdws = new LinkedHashMap<>();
    private final Map<String, PgServer> servers = new LinkedHashMap<>();
    private final Map<String, PgUserMapping> userMappings = new LinkedHashMap<>();
    private final Map<String, PgCast> casts = new LinkedHashMap<>();
    private final Map<String, PgSchema> schemas = new LinkedHashMap<>();

    /**
     * Creates a new PostgreSQL database.
     */
    public PgDatabase() {
        super("DB_name_placeholder");
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
        l.add(extensions.values());
        l.add(eventTriggers.values());
        l.add(fdws.values());
        l.add(servers.values());
        l.add(userMappings.values());
        l.add(casts.values());
    }

    @Override
    public AbstractStatement getChild(String name, DbObjType type) {
        return switch (type) {
            case SCHEMA -> getSchema(name);
            case EXTENSION -> getExtension(name);
            case EVENT_TRIGGER -> getEventTrigger(name);
            case FOREIGN_DATA_WRAPPER -> getForeignDW(name);
            case SERVER -> getServer(name);
            case USER_MAPPING -> getChildByName(userMappings, name);
            case CAST -> getCast(name);
            default -> null;
        };
    }

    @Override
    public Collection<IStatement> getChildrenByType(DbObjType type) {
        return switch (type) {
            case SCHEMA -> Collections.unmodifiableCollection(schemas.values());
            case EXTENSION -> Collections.unmodifiableCollection(extensions.values());
            case EVENT_TRIGGER -> Collections.unmodifiableCollection(eventTriggers.values());
            case FOREIGN_DATA_WRAPPER -> Collections.unmodifiableCollection(fdws.values());
            case SERVER -> Collections.unmodifiableCollection(servers.values());
            case USER_MAPPING -> Collections.unmodifiableCollection(userMappings.values());
            case CAST -> Collections.unmodifiableCollection(casts.values());
            default -> List.of();
        };
    }

    /**
     * Returns schema of given name or null if the schema has not been found. If schema name is null then default schema
     * is returned.
     *
     * @param name schema name or null which means default schema
     * @return found schema or null
     */
    @Override
    public PgSchema getSchema(final String name) {
        if (name == null) {
            return getDefaultSchema();
        }

        return getChildByName(schemas, name);
    }

    @Override
    public PgSchema getDefaultSchema() {
        return defaultSchema;
    }

    @Override
    public void setDefaultSchema(final String name) {
        defaultSchema = getSchema(name);
    }

    @Override
    public void addChild(IStatement st) {
        DbObjType type = st.getStatementType();
        switch (type) {
            case SCHEMA:
                addSchema((PgSchema) st);
                break;
            case EXTENSION:
                addExtension((PgExtension) st);
                break;
            case EVENT_TRIGGER:
                addEventTrigger((PgEventTrigger) st);
                break;
            case FOREIGN_DATA_WRAPPER:
                addForeignDW((PgForeignDataWrapper) st);
                break;
            case SERVER:
                addServer((PgServer) st);
                break;
            case CAST:
                addCast((PgCast) st);
                break;
            case USER_MAPPING:
                addUserMapping((PgUserMapping) st);
                break;
            default:
                throw new IllegalArgumentException("Unsupported child type: " + type);
        }
    }

    /**
     * Adds a schema to this database.
     *
     * @param schema the schema to add
     */
    private void addSchema(final PgSchema schema) {
        addUnique(schemas, schema);
    }

    /**
     * Returns extension of given name or null if the extension has not been found.
     *
     * @param name extension name
     * @return found extension or null
     */
    public PgExtension getExtension(final String name) {
        return getChildByName(extensions, name);
    }

    private void addExtension(final PgExtension extension) {
        addUnique(extensions, extension);
    }

    public PgEventTrigger getEventTrigger(final String name) {
        return getChildByName(eventTriggers, name);
    }

    private void addEventTrigger(final PgEventTrigger et) {
        addUnique(eventTriggers, et);
    }

    /**
     * Returns foreign data wrapper of given name or null if the foreign data wrapper has not been found.
     *
     * @param name foreign data wrapper name
     * @return found foreign data wrapper or null
     */
    public PgForeignDataWrapper getForeignDW(final String name) {
        return getChildByName(fdws, name);
    }

    private void addForeignDW(final PgForeignDataWrapper fDW) {
        addUnique(fdws, fDW);
    }

    public PgServer getServer(final String name) {
        return getChildByName(servers, name);
    }

    private void addServer(final PgServer server) {
        addUnique(servers, server);
    }

    private void addUserMapping(final PgUserMapping userMapping) {
        addUnique(userMappings, userMapping);
    }

    /**
     * Returns cast of given name or null if the cast has not been found.
     *
     * @param name cast name
     * @return found cast or null
     */
    public PgCast getCast(final String name) {
        return casts.get(name);
    }

    private void addCast(final PgCast cast) {
        addUnique(casts, cast);
    }

    @Override
    public void concat(IStatement st) {
        DbObjType type = st.getStatementType();
        String name = st.getName();
        if (type != DbObjType.SCHEMA || !PgConsts.DEFAULT_SCHEMA.equals(name) || st.hasChildren()) {
            // skip empty public schema
            IDatabase.super.concat(st);
        }
    }

    /**
     * Sorts columns in all tables within all schemas of this database.
     * This is used to ensure consistent column ordering in inherited tables.
     */
    public void sortColumns() {
        for (PgSchema schema : getSchemas()) {
            schema.getTables().forEach(t -> t.sortColumns());
        }
    }

    /**
     * Getter for {@link #schemas}. The list cannot be modified.
     *
     * @return {@link #schemas}
     */
    @Override
    public Collection<PgSchema> getSchemas() {
        return Collections.unmodifiableCollection(schemas.values());
    }

    private IOperator resolveOperatorCall(PgSchema abstractSchema, String table) {
        PgSchema schema = abstractSchema;
        IOperator oper = null;
        if (table.indexOf('(') != -1) {
            oper = schema.getOperator(table);
        }
        if (oper != null) {
            return oper;
        }

        int found = 0;
        for (IOperator o : schema.getOperators()) {
            if (o.getBareName().equals(table)) {
                ++found;
                oper = o;
            }
        }
        return found == 1 ? oper : null;
    }

    @Override
    public final AbstractStatement getStatement(ObjectReference reference) {
        DbObjType type = reference.type();
        if (type == DbObjType.DATABASE) {
            return this;
        }

        if (type.in(DbObjType.SCHEMA, DbObjType.EXTENSION, DbObjType.FOREIGN_DATA_WRAPPER, DbObjType.EVENT_TRIGGER,
                DbObjType.SERVER, DbObjType.USER_MAPPING, DbObjType.CAST)) {
            return getChild(reference.schema(), type);
        }

        PgSchema s = getSchema(reference.schema());
        if (s == null) {
            return null;
        }

        return switch (type) {
            case DOMAIN, SEQUENCE, VIEW, COLLATION, FTS_PARSER, FTS_TEMPLATE, FTS_DICTIONARY, FTS_CONFIGURATION,
                 STATISTICS ->
                    s.getChild(reference.table(), type);
            case TYPE -> (AbstractStatement) resolveTypeCall(s, reference.table());
            case FUNCTION, PROCEDURE, AGGREGATE -> s.getFunction(reference.table());
            case OPERATOR -> (AbstractStatement) resolveOperatorCall(s, reference.table());
            case TABLE -> (AbstractStatement) s.getRelation(reference.table());
            case INDEX -> s.getIndexByName(reference.table());
            // handled in getStatement, left here for consistency
            case COLUMN -> {
                PgAbstractTable t = s.getTable(reference.table());
                yield t == null ? null : t.getColumn(reference.column());
            }
            case CONSTRAINT, TRIGGER, RULE, POLICY -> {
                var sc = s.getStatementContainer(reference.table());
                yield sc == null ? null : sc.getChild(reference.column(), type);
            }
            default -> throw new IllegalStateException("Unhandled DbObjType: " + type);
        };
    }

    private IStatement resolveTypeCall(PgSchema s, String table) {
        AbstractStatement st = s.getChild(table, DbObjType.TYPE);
        if (st != null) {
            return st;
        }
        st = s.getChild(table, DbObjType.DOMAIN);
        if (st != null) {
            return st;
        }
        // every "selectable" relation can be used as a type
        // getRelation should only look for "selectable" relations
        return s.getRelation(table);
    }

    @Override
    public void computeChildrenHash(Hasher hasher) {
        hasher.putUnordered(extensions);
        hasher.putUnordered(eventTriggers);
        hasher.putUnordered(fdws);
        hasher.putUnordered(servers);
        hasher.putUnordered(casts);
    }

    @Override
    public boolean compareChildren(AbstractStatement obj) {
        if (obj instanceof PgDatabase db && super.compareChildren(obj)) {
            return extensions.equals(db.extensions)
                    && eventTriggers.equals(db.eventTriggers)
                    && fdws.equals(db.fdws)
                    && servers.equals(db.servers)
                    && casts.equals(db.casts);
        }

        return false;
    }

    @Override
    public void computeHash(Hasher hasher) {
        // has only child objects
    }

    @Override
    protected PgDatabase getCopy() {
        PgDatabase dbDst = new PgDatabase();
        dbDst.setVersion(version);
        return dbDst;
    }

    public void setVersion(PgSupportedVersion version) {
        this.version = version;
    }

    @Override
    public ISupportedVersion getVersion() {
        return version;
    }

    @Override
    public void addOverride(ObjectOverride override) {
        overrides.add(override);
    }

    @Override
    public List<ObjectOverride> getOverrides() {
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

    @Override
    public void addAnalysisLauncher(IAnalysisLauncher launcher) {
        analysisLaunchers.add(launcher);
    }

    @Override
    public void addReference(String fileName, ObjectLocation loc) {
        objReferences.computeIfAbsent(fileName, k -> new LinkedHashSet<>()).add(loc);
    }

    /**
     * Clears all analysis launchers and trims the internal list to size.
     */
    @Override
    public void clearAnalysisLaunchers() {
        analysisLaunchers.clear();
        analysisLaunchers.trimToSize();
    }
}
