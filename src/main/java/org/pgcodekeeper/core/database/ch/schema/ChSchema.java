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

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;

import java.util.*;
import java.util.stream.Stream;

/**
 * Represents a ClickHouse database schema (database in ClickHouse terms).
 * Contains tables, views, dictionaries and has an associated engine type.
 */
public class ChSchema extends ChAbstractStatement implements ISchema, IStatementContainer {

    private final Map<String, ChTable> tables = new LinkedHashMap<>();
    private final Map<String, ChView> views = new LinkedHashMap<>();
    private final Map<String, ChDictionary> dictionaries = new LinkedHashMap<>();

    private String engine = "Atomic";

    /**
     * Creates a new ClickHouse schema with the specified name.
     *
     * @param name the name of the schema
     */
    public ChSchema(String name) {
        super(name);
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        var sb = new StringBuilder();
        sb.append("CREATE DATABASE ");
        appendIfNotExists(sb, script.getSettings());
        sb.append(getQualifiedName()).append("\nENGINE = ").append(engine);
        if (getComment() != null) {
            sb.append("\nCOMMENT ").append(getComment());
        }
        script.addStatement(sb);
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        if (!compareUnalterable((ChSchema) newCondition)) {
            return ObjectState.RECREATE;
        }
        return ObjectState.NOTHING;
    }

    @Override
    public void getDropSQL(SQLScript script, boolean generateExists) {
        final StringBuilder sb = new StringBuilder();
        sb.append("DROP DATABASE ");
        if (generateExists) {
            sb.append(IF_EXISTS);
        }
        appendFullName(sb);
        script.addStatement(sb);
    }

    public void setEngine(String engine) {
        this.engine = engine;
        resetHash();
    }

    @Override
    public Stream<IRelation> getRelations() {
        return Stream.concat(Stream.concat(tables.values().stream(), views.values().stream()),
                dictionaries.values().stream());
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

    @Override
    public void fillChildrenList(List<Collection<? extends AbstractStatement>> l) {
        l.add(tables.values());
        l.add(views.values());
        l.add(dictionaries.values());
    }

    @Override
    public IRelation getRelation(String name) {
        IRelation result = tables.get(name);
        if (result == null) {
            result = views.get(name);
        }
        if (result == null) {
            result = dictionaries.get(name);
        }
        return result;
    }

    @Override
    public AbstractStatement getChild(String name, DbObjType type) {
        return switch (type) {
            case TABLE -> getTable(name);
            case VIEW -> getChildByName(views, name);
            case DICTIONARY -> getChildByName(dictionaries, name);
            default -> null;
        };
    }

    @Override
    public Collection<IStatement> getChildrenByType(DbObjType type) {
        return switch (type) {
            case TABLE -> Collections.unmodifiableCollection(tables.values());
            case VIEW -> Collections.unmodifiableCollection(views.values());
            case DICTIONARY -> Collections.unmodifiableCollection(dictionaries.values());
            default -> List.of();
        };
    }

    @Override
    public void addChild(IStatement st) {
        DbObjType type = st.getStatementType();
        switch (type) {
            case DICTIONARY:
                addUnique(dictionaries, (ChDictionary) st);
                break;
            case TABLE:
                addUnique(tables, (ChTable) st);
                break;
            case VIEW:
                addUnique(views, (ChView) st);
                break;
            default:
                throw new IllegalArgumentException("Unsupported child type: " + type);
        }
    }

    /**
     * Gets a statement container by name.
     *
     * @param name the name of the container to find
     * @return the statement container with the given name, or null if not found
     */
    @Override
    public IStatementContainer getStatementContainer(String name) {
        IStatementContainer container = tables.get(name);
        return container == null ? views.get(name) : container;
    }

    /**
     * Gets a stream of all statement containers in this schema.
     *
     * @return a stream of statement containers
     */
    public Stream<IStatementContainer> getStatementContainers() {
        return Stream.concat(tables.values().stream(), views.values().stream());
    }

    /**
     * Finds an index by name across all tables and views in this schema.
     *
     * @param indexName the name of the index to find
     * @return the index with the given name, or null if not found
     */
    public ChIndex getIndexByName(String indexName) {
        return (ChIndex) getStatementContainers()
                .map(c -> c.getChild(indexName, DbObjType.INDEX))
                .filter(Objects::nonNull)
                .findAny().orElse(null);
    }

    /**
     * Finds a constraint by name across all tables and views in this schema.
     *
     * @param constraintName the name of the constraint to find
     * @return the constraint with the given name, or null if not found
     */
    public ChConstraint getConstraintByName(String constraintName) {
        return (ChConstraint) getStatementContainers()
                .map(c -> c.getChild(constraintName, DbObjType.CONSTRAINT))
                .filter(Objects::nonNull)
                .findAny().orElse(null);
    }

    public ChTable getTable(String name) {
        return getChildByName(tables, name);
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.put(engine);
    }

    @Override
    protected void computeChildrenHash(Hasher hasher) {
        hasher.putUnordered(views);
        hasher.putUnordered(tables);
        hasher.putUnordered(dictionaries);
    }

    @Override
    public boolean compare(IStatement obj) {
        if (this == obj) {
            return true;
        }
        return obj instanceof ChSchema schema
                && super.compare(schema)
                && compareUnalterable(schema);
    }

    private boolean compareUnalterable(ChSchema newSchema) {
        return Objects.equals(engine, newSchema.engine)
                && Objects.equals(comment, newSchema.comment);
    }

    @Override
    public boolean compareChildren(AbstractStatement obj) {
        if (obj instanceof ChSchema schema) {
            return views.equals(schema.views)
                    && tables.equals(schema.tables)
                    && dictionaries.equals(schema.dictionaries);
        }
        return false;
    }

    @Override
    protected ChSchema getCopy() {
        var schema = new ChSchema(name);
        schema.setEngine(engine);
        return schema;
    }
}
