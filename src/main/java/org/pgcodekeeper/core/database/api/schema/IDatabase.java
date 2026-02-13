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
package org.pgcodekeeper.core.database.api.schema;

import org.pgcodekeeper.core.database.api.jdbc.ISupportedVersion;
import org.pgcodekeeper.core.database.api.launcher.IAnalysisLauncher;
import org.pgcodekeeper.core.script.SQLScript;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Interface representing a database containing schemas.
 * Provides access to database schemas and extends statement container functionality.
 */
public interface IDatabase extends IStatementContainer {
    /**
     * Gets all schemas in this database.
     *
     * @return a collection of schemas
     */
    Collection<? extends ISchema> getSchemas();

    /**
     * Gets a schema by name.
     *
     * @param name the schema name
     * @return the schema with the given name, or null if not found
     */
    ISchema getSchema(String name);

    default boolean containsSchema(final String name) {
        return getSchema(name) != null;
    }

    /**
     * @param objectReference - object reference
     * @return object from database by reference, or null if not found
     *
     * @throws IllegalStateException if reference type is not supported by database
     */
    IStatement getStatement(ObjectReference objectReference);

    @Override
    default IDatabase getDatabase() {
        return this;
    }

    @Override
    default DbObjType getStatementType() {
        return DbObjType.DATABASE;
    }

    /**
     * @return database version
     */
    ISupportedVersion getVersion();

    @Override
    default void getCreationSQL(SQLScript script) {
        // no action
    }

    @Override
    default ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        return ObjectState.NOTHING;
    }

    @Override
    default void getDropSQL(SQLScript script, boolean optionExists) {
        // no action
    }

    ISchema getDefaultSchema();

    void setDefaultSchema(String name);

    /**
     * Adds a library database to this database, merging its objects and analysis launchers.
     *
     * @param lib     the library database to add
     * @param libName the name of the library
     * @param owner   the owner to set for owned objects
     */
    default void addLib(IDatabase lib, String libName, String owner) {
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

        lib.getAnalysisLaunchers().stream()
                .filter(st -> st.getStmt().getParent() != null)
                .forEach(l -> {
                    l.updateStmt(this);
                    addAnalysisLauncher(l);
                });

        getOverrides().addAll(lib.getOverrides());
        lib.getObjReferences().forEach(getObjReferences()::putIfAbsent);
    }

    /**
     * Adds library object to database with override
     *
     * @param st the library object to add
     */
    default void concat(IStatement st) {
        DbObjType type = st.getStatementType();
        IStatement parent = st.getParent();
        String parentName = parent.getName();
        IStatementContainer cont;

        if (st instanceof ISubElement) {
            cont = getSchema(parent.getParent().getName()).getStatementContainer(parentName);
        } else if (st instanceof ISearchPath) {
            cont = getSchema(parentName);
        } else {
            cont = this;
        }

        String name = st.getName();
        IStatement orig = cont.getChild(name, type);
        if (orig == null) {
            cont.addChild(st.shallowCopy());
        }

        if (orig != null && !orig.compare(st)) {
            addOverride(new ObjectOverride(orig, st));
        }
    }

    /**
     * Adds object override

     * @param override object override
     */
    void addOverride(ObjectOverride override);

    /**
     * Gets the list of object overrides for this database.
     *
     * @return the list of overrides
     */
    Collection<ObjectOverride> getOverrides();

    /**
     * @return all object references for this database
     */
    Map<String, Set<ObjectLocation>> getObjReferences();

    /**
     * @return all analysis launcher for this database
     */
    List<IAnalysisLauncher> getAnalysisLaunchers();

    /**
     * Add 'analysis launcher' for deferred analyze.
     *
     * @param launcher launcher that contains almost everything needed to analyze a statement contained in it
     */
    void addAnalysisLauncher(IAnalysisLauncher launcher);

    /**
     * Adds an object reference to the specified file.
     *
     * @param fileName the file name to associate with the location
     * @param loc      the object location to add
     */
    void addReference(String fileName, ObjectLocation loc);

    /**
     * Clears all analysis launchers and trims the internal list to size.
     */
    void clearAnalysisLaunchers();

    /**
     * Creates a map of all database objects with their qualified names as keys.
     *
     * @return a map of qualified names to statements
     */
    default Map<String, IStatement> listObjects() {
        Map<String, IStatement> statements = new HashMap<>();
        getDescendants()
                .flatMap(ITable::columnAdder)
                .forEach(st -> statements.put(st.getQualifiedName(), st));
        return statements;
    }

}
