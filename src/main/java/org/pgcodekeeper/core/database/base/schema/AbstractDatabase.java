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
package org.pgcodekeeper.core.database.base.schema;

import java.util.*;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.parser.launcher.AbstractAnalysisLauncher;
import org.pgcodekeeper.core.database.pg.jdbc.SupportedPgVersion;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;

/**
 * Abstract base class representing a database schema.
 * Contains schemas, handles database-specific operations, and manages object references
 * and analysis launchers for PostgreSQL, Microsoft SQL, and ClickHouse databases.
 */
@Deprecated
public abstract class AbstractDatabase extends AbstractStatement implements IDatabase {

    private SupportedPgVersion version;

    /**
     * Current default schema.
     */
    private AbstractSchema defaultSchema;
    private final Map<String, AbstractSchema> schemas = new LinkedHashMap<>();

    private final List<ObjectOverride> overrides = new ArrayList<>();

    // Contains object references
    private final Map<String, Set<ObjectLocation>> objReferences = new HashMap<>();
    // Contains analysis launchers for all statements
    // (used for launch analyze and getting dependencies).
    private final ArrayList<AbstractAnalysisLauncher> analysisLaunchers = new ArrayList<>();

    protected AbstractDatabase() {
        super("DB_name_placeholder");
    }

    /**
     * Gets the list of object overrides for this database.
     *
     * @return the list of overrides
     */
    public List<ObjectOverride> getOverrides() {
        return overrides;
    }

    @Override
    public final DbObjType getStatementType() {
        return DbObjType.DATABASE;
    }

    @Override
    public AbstractDatabase getDatabase() {
        return this;
    }

    public SupportedPgVersion getVersion() {
        return version != null ? version : SupportedPgVersion.VERSION_14;
    }

    public void setVersion(SupportedPgVersion version) {
        this.version = version;
    }

    public Map<String, Set<ObjectLocation>> getObjReferences() {
        return objReferences;
    }

    public Set<ObjectLocation> getObjReferences(String name) {
        return objReferences.getOrDefault(name, Collections.emptySet());
    }

    /**
     * Adds an object reference to the specified file.
     *
     * @param fileName the file name to associate with the location
     * @param loc      the object location to add
     */
    public void addReference(String fileName, ObjectLocation loc) {
        objReferences.computeIfAbsent(fileName, k -> new LinkedHashSet<>()).add(loc);
    }

    public List<AbstractAnalysisLauncher> getAnalysisLaunchers() {
        return analysisLaunchers;
    }

    /**
     * Clears all analysis launchers and trims the internal list to size.
     */
    public void clearAnalysisLaunchers() {
        analysisLaunchers.clear();
        analysisLaunchers.trimToSize();
    }

    /**
     * Add 'analysis launcher' for deferred analyze.
     *
     * @param launcher launcher that contains almost everything needed to analyze a statement contained in it
     */
    public void addAnalysisLauncher(AbstractAnalysisLauncher launcher) {
        analysisLaunchers.add(launcher);
    }

    @Override
    protected void fillDescendantsList(List<Collection<? extends AbstractStatement>> l) {
        fillChildrenList(l);
        for (AbstractSchema schema : schemas.values()) {
            schema.fillDescendantsList(l);
        }
    }

    /**
     * Getter for {@link #schemas}. The list cannot be modified.
     *
     * @return {@link #schemas}
     */
    @Override
    public Collection<AbstractSchema> getSchemas() {
        return Collections.unmodifiableCollection(schemas.values());
    }

    /**
     * Adds a schema to this database.
     *
     * @param schema the schema to add
     */
    public void addSchema(final AbstractSchema schema) {
        addUnique(schemas, schema);
    }

    public void setDefaultSchema(final String name) {
        defaultSchema = getSchema(name);
    }

    public AbstractSchema getDefaultSchema() {
        return defaultSchema;
    }

    public boolean containsSchema(final String name) {
        return getSchema(name) != null;
    }

    /**
     * Returns schema of given name or null if the schema has not been found. If schema name is null then default schema
     * is returned.
     *
     * @param name schema name or null which means default schema
     * @return found schema or null
     */
    @Override
    public AbstractSchema getSchema(final String name) {
        if (name == null) {
            return getDefaultSchema();
        }

        return getChildByName(schemas, name);
    }

    @Override
    protected void fillChildrenList(List<Collection<? extends AbstractStatement>> l) {
        l.add(schemas.values());
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        // no action
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        return ObjectState.NOTHING;
    }

    @Override
    public void getDropSQL(SQLScript script, boolean optionExists) {
    }

    @Override
    public void computeHash(Hasher hasher) {
        // has only child objects
    }

    @Override
    public void computeChildrenHash(Hasher hasher) {
        hasher.putUnordered(schemas);
    }

    /**
     * Adds a library database to this database, merging its objects and analysis launchers.
     *
     * @param lib     the library database to add
     * @param libName the name of the library
     * @param owner   the owner to set for owned objects
     */
    public void addLib(AbstractDatabase lib, String libName, String owner) {
        lib.getDescendants().forEach(st -> {
            // do not override dependent library name
            if (libName != null && st.getLibName() == null) {
                st.setLibName(libName);
            }
            if (st.isOwned() && owner != null && !owner.isEmpty()) {
                st.setOwner(owner);
            }
            concat(st);
        });

        lib.analysisLaunchers.stream()
                .filter(st -> st.getStmt().getParent() != null)
                .forEach(l -> {
                    l.updateStmt(this);
                    analysisLaunchers.add(l);
                });

        overrides.addAll(lib.overrides);
        lib.objReferences.forEach(objReferences::putIfAbsent);
    }

    protected void addOverride(ObjectOverride override) {
        overrides.add(override);
    }

    protected void concat(AbstractStatement st) {
        DbObjType type = st.getStatementType();
        AbstractStatement parent = st.parent;
        String parentName = parent.getName();
        IStatementContainer cont;

        if (isFirstLevelType(type)) {
            cont = this;
        } else if (st instanceof ISubElement) {
            cont = getSchema(parent.parent.getName()).getStatementContainer(parentName);
        } else {
            cont = getSchema(parentName);
        }

        String name = st.getName();
        AbstractStatement orig = (AbstractStatement) cont.getChild(name, type);
        if (orig == null) {
            cont.addChild(st.shallowCopy());
        }

        if (orig != null && !orig.compare(st)) {
            addOverride(new ObjectOverride(orig, st));
        }
    }

    protected abstract boolean isFirstLevelType(DbObjType type);

    /**
     * Creates a map of all database objects with their qualified names as keys.
     *
     * @param db the database to list objects from
     * @return a map of qualified names to statements
     */
    public static Map<String, IStatement> listObjects(IDatabase db) {
        Map<String, IStatement> statements = new HashMap<>();
        db.getDescendants().flatMap(AbstractTable::columnAdder)
                .forEach(st -> statements.put(st.getQualifiedName(), st));
        return statements;
    }

    /**
     * Copies analysis launchers from another database to this one.
     *
     * @param db the database to copy launchers from
     */
    public void copyLaunchers(AbstractDatabase db) {
        analysisLaunchers.addAll(db.analysisLaunchers);
    }

    @Override
    public boolean compare(IStatement obj) {
        return obj instanceof AbstractDatabase && super.compare(obj);
    }

    @Override
    public boolean compareChildren(AbstractStatement obj) {
        return obj instanceof AbstractDatabase db && schemas.equals(db.schemas);
    }

    @Override
    public final AbstractDatabase shallowCopy() {
        AbstractDatabase dbDst = getDatabaseCopy();
        dbDst.setVersion(version);
        copyBaseFields(dbDst);
        return dbDst;
    }

    protected abstract AbstractDatabase getDatabaseCopy();

    /**
     * Resolves and returns a database statement based on the provided generic column specification.
     *
     * @param gc the generic column specification containing type and naming information
     * @return the resolved statement, or null if not found
     */
    public final AbstractStatement getStatement(GenericColumn gc) {
        DbObjType type = gc.type();
        if (type == DbObjType.DATABASE) {
            return this;
        }

        if (isFirstLevelType(type)) {
            return (AbstractStatement) getChild(gc.schema(), type);
        }

        AbstractSchema s = getSchema(gc.schema());
        if (s == null) {
            return null;
        }

        return switch (type) {
            case DOMAIN, SEQUENCE, VIEW, COLLATION, FTS_PARSER, FTS_TEMPLATE, FTS_DICTIONARY, FTS_CONFIGURATION ->
                    s.getChild(gc.table(), type);
            case STATISTICS -> resolveStatistics(s, gc, type);
            case TYPE -> (AbstractStatement) resolveTypeCall(s, gc.table());
            case FUNCTION, PROCEDURE, AGGREGATE -> (AbstractStatement) resolveFunctionCall(s, gc.table());
            case OPERATOR -> (AbstractStatement) resolveOperatorCall(s, gc.table());
            case TABLE -> (AbstractStatement) s.getRelation(gc.table());
            case INDEX -> s.getIndexByName(gc.table());
            // handled in getStatement, left here for consistency
            case COLUMN -> {
                AbstractTable t = s.getTable(gc.table());
                yield t == null ? null : t.getColumn(gc.column());
            }
            case CONSTRAINT, TRIGGER, RULE, POLICY -> {
                var sc = s.getStatementContainer(gc.table());
                yield sc == null ? null : sc.getChild(gc.column(), type);
            }
            default -> throw new IllegalStateException("Unhandled DbObjType: " + type);
        };
    }

    protected AbstractStatement resolveStatistics(AbstractSchema s, GenericColumn gc, DbObjType type) {
        return s.getChild(gc.table(), type);
    }

    private IStatement resolveTypeCall(AbstractSchema s, String table) {
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

    private IFunction resolveFunctionCall(AbstractSchema schema, String table) {
        if (schema.getDbType() == DatabaseType.PG) {
            return schema.getFunction(table);
        }

        for (IFunction f : schema.getFunctions()) {
            if (f.getBareName().equals(table)) {
                return f;
            }
        }
        return null;
    }

    protected IOperator resolveOperatorCall(AbstractSchema schema, String table) {
        return null;
    }
}