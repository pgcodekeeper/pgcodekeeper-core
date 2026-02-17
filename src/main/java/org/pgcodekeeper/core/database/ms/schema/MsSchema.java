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

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;

import java.util.*;
import java.util.stream.Stream;

/**
 * Represents a Microsoft SQL schema that contains database objects like tables, views, functions, and procedures.
 * Provides SQL generation for schema creation and management.
 */
public class MsSchema extends MsAbstractStatement implements ISchema {

    private final Map<String, MsAbstractCommonFunction> functions = new LinkedHashMap<>();
    private final Map<String, MsSequence> sequences = new LinkedHashMap<>();
    private final Map<String, MsTable> tables = new LinkedHashMap<>();
    private final Map<String, MsView> views = new LinkedHashMap<>();
    private final Map<String, MsType> types = new LinkedHashMap<>();

    /**
     * Creates a new Microsoft SQL schema.
     *
     * @param name the schema name
     */
    public MsSchema(String name) {
        super(name);
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        final StringBuilder sbSQL = new StringBuilder();
        sbSQL.append("CREATE SCHEMA ");
        sbSQL.append(getQuotedName());
        if (owner != null) {
            sbSQL.append("\nAUTHORIZATION ").append(quote(owner));
        }
        script.addStatement(sbSQL);
        appendPrivileges(script);
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        int startSize = script.getSize();
        var newSchema = (MsSchema) newCondition;
        appendAlterOwner(newSchema, script);
        alterPrivileges(newSchema, script);
        return getObjectState(script, startSize);
    }

    @Override
    public Stream<IRelation> getRelations() {
        return Stream.concat(Stream.concat(tables.values().stream(), views.values().stream()),
                sequences.values().stream());
    }

    /**
     * @return found relation or null if no such relation has been found
     */
    @Override
    public IRelation getRelation(String name) {
        IRelation result = getTable(name);
        if (result == null) {
            result = getView(name);
        }
        if (result == null) {
            result = getSequence(name);
        }
        return result;
    }

    @Override
    public void addChild(IStatement st) {
        DbObjType type = st.getStatementType();
        switch (type) {
            case FUNCTION, PROCEDURE:
                addFunction((MsAbstractCommonFunction) st);
                break;
            case SEQUENCE:
                addSequence((MsSequence) st);
                break;
            case TABLE:
                addTable((MsTable) st);
                break;
            case TYPE:
                addType((MsType) st);
                break;
            case VIEW:
                addView((MsView) st);
                break;
            default:
                throw new IllegalArgumentException("Unsupported child type: " + type);
        }
    }

    @Override
    public IStatement getChild(String name, DbObjType type) {
        return switch (type) {
            case FUNCTION, PROCEDURE -> getFunction(name);
            case SEQUENCE -> getSequence(name);
            case TYPE -> getType(name);
            case TABLE -> getTable(name);
            case VIEW -> getView(name);
            default -> null;
        };
    }

    @Override
    public Collection<IStatement> getChildrenByType(DbObjType type) {
        return switch (type) {
            case FUNCTION, PROCEDURE -> Collections.unmodifiableCollection(functions.values());
            case SEQUENCE -> Collections.unmodifiableCollection(sequences.values());
            case TYPE -> Collections.unmodifiableCollection(types.values());
            case TABLE -> Collections.unmodifiableCollection(tables.values());
            case VIEW -> Collections.unmodifiableCollection(views.values());
            default -> List.of();
        };
    }

    @Override
    public void fillChildrenList(List<Collection<? extends AbstractStatement>> l) {
        l.add(functions.values());
        l.add(sequences.values());
        l.add(tables.values());
        l.add(views.values());
        l.add(types.values());
    }

    @Override
    public void fillDescendantsList(List<Collection<? extends AbstractStatement>> l) {
        fillChildrenList(l);
        for (var table : tables.values()) {
            table.fillDescendantsList(l);
        }
        for (var view : views.values()) {
            view.fillDescendantsList(l);
        }
    }

    /**
     * Finds table according to specified table {@code name}.
     *
     * @param name name of the table to be searched
     * @return found table or null if no such table has been found
     */
    public MsTable getTable(final String name) {
        return getChildByName(tables, name);
    }

    /**
     * Finds sequence according to specified sequence {@code name}.
     *
     * @param name name of the sequence to be searched
     * @return found sequence or null if no such sequence has been found
     */
    public MsSequence getSequence(final String name) {
        return getChildByName(sequences, name);
    }

    /**
     * Finds function according to specified function {@code signature}.
     *
     * @param signature signature of the function to be searched
     * @return found function or null if no such function has been found
     */
    public MsAbstractCommonFunction getFunction(final String signature) {
        return getChildByName(functions, signature);
    }

    /**
     * Finds type according to specified type {@code name}.
     *
     * @param name name of the type to be searched
     * @return found type or null if no such type has been found
     */
    public MsType getType(final String name) {
        return getChildByName(types, name);
    }

    /**
     * Finds view according to specified view {@code name}.
     *
     * @param name name of the view to be searched
     * @return found view or null if no such view has been found
     */
    public MsView getView(final String name) {
        return getChildByName(views, name);
    }

    /**
     * Gets a statement container by name.
     *
     * @param name the name of the container to find
     * @return the statement container with the given name, or null if not found
     */
    @Override
    public MsAbstractStatementContainer getStatementContainer(String name) {
        MsAbstractStatementContainer container = getTable(name);
        return container == null ? getView(name) : container;
    }

    /**
     * Adds a function to this schema.
     *
     * @param function the function to add
     */
    public void addFunction(final MsAbstractCommonFunction function) {
        addUnique(functions, function);
    }

    /**
     * Adds a sequence to this schema.
     *
     * @param sequence the sequence to add
     */
    public void addSequence(final MsSequence sequence) {
        addUnique(sequences, sequence);
    }

    /**
     * Adds a table to this schema.
     *
     * @param table the table to add
     */
    public void addTable(final MsTable table) {
        addUnique(tables, table);
    }

    /**
     * Adds a view to this schema.
     *
     * @param view the table to add
     */
    public void addView(final MsView view) {
        addUnique(views, view);
    }

    /**
     * Adds a type to this schema.
     *
     * @param type the table to add
     */
    public void addType(final MsType type) {
        addUnique(types, type);
    }

    /**
     * Finds an index by name across all tables and views in this schema.
     *
     * @param indexName the name of the index to find
     * @return the index with the given name, or null if not found
     */
    public MsIndex getIndexByName(String indexName) {
        return (MsIndex) getStatementContainers()
                .map(c -> c.getChild(indexName, DbObjType.INDEX))
                .filter(Objects::nonNull)
                .findAny().orElse(null);
    }

    /**
     * Gets a stream of all statement containers in this schema.
     *
     * @return a stream of statement containers
     */
    public Stream<MsAbstractStatementContainer> getStatementContainers() {
        return Stream.concat(tables.values().stream(), views.values().stream());
    }

    /**
     * Getter for {@link #functions}. The list cannot be modified.
     *
     * @return {@link #functions}
     */
    public Collection<IFunction> getFunctions() {
        return Collections.unmodifiableCollection(functions.values());
    }

    @Override
    public void computeHash(Hasher hasher) {
        // all hashable fields in AbstractStatement
    }

    @Override
    protected void computeChildrenHash(Hasher hasher) {
        hasher.putUnordered(sequences);
        hasher.putUnordered(functions);
        hasher.putUnordered(views);
        hasher.putUnordered(tables);
        hasher.putUnordered(types);
    }

    @Override
    public boolean compare(IStatement obj) {
        return this == obj || (obj instanceof MsSchema && super.compare(obj));
    }

    @Override
    public boolean compareChildren(AbstractStatement obj) {
        return obj instanceof MsSchema schema
                && sequences.equals(schema.sequences)
                && functions.equals(schema.functions)
                && views.equals(schema.views)
                && tables.equals(schema.tables)
                && types.equals(schema.types);
    }

    @Override
    protected MsSchema getCopy() {
        return new MsSchema(name);
    }
}
