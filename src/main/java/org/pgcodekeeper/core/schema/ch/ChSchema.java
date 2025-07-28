/*******************************************************************************
 * Copyright 2017-2025 TAXTELECOM, LLC
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
package org.pgcodekeeper.core.schema.ch;

import org.pgcodekeeper.core.DatabaseType;
import org.pgcodekeeper.core.hashers.Hasher;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.schema.*;
import org.pgcodekeeper.core.script.SQLScript;

import java.util.*;
import java.util.stream.Stream;

/**
 * Represents a ClickHouse database schema (database in ClickHouse terms).
 * Contains tables, views, dictionaries and has an associated engine type.
 */
public final class ChSchema extends AbstractSchema {

    private String engine = "Atomic";
    private final Map<String, ChDictionary> dictionaries = new LinkedHashMap<>();

    /**
     * Creates a new ClickHouse schema with the specified name.
     *
     * @param name the name of the schema
     */
    public ChSchema(String name) {
        super(name);
    }

    public void setEngine(String engine) {
        this.engine = engine;
        resetHash();
    }

    @Override
    public Stream<IRelation> getRelations() {
        return Stream.concat(super.getRelations(), dictionaries.values().stream());
    }

    @Override
    public IRelation getRelation(String name) {
        IRelation result = super.getRelation(name);
        return result != null ? result : getDictionary(name);
    }

    private void addDictionary(final ChDictionary dictionary) {
        addUnique(dictionaries, dictionary);
    }

    @Override
    protected void fillChildrenList(List<Collection<? extends PgStatement>> l) {
        super.fillChildrenList(l);
        l.add(dictionaries.values());
    }

    @Override
    public PgStatement getChild(String name, DbObjType type) {
        if (type == DbObjType.DICTIONARY) {
            return getDictionary(name);
        }
        return super.getChild(name, type);
    }

    private ChDictionary getDictionary(String name) {
        return getChildByName(dictionaries, name);
    }

    @Override
    public void addChild(IStatement st) {
        DbObjType type = st.getStatementType();
        if (type == DbObjType.DICTIONARY) {
            addDictionary((ChDictionary) st);
            return;
        }
        super.addChild(st);
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
    public ObjectState appendAlterSQL(PgStatement newCondition, SQLScript script) {
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

    @Override
    public DatabaseType getDbType() {
        return DatabaseType.CH;
    }

    @Override
    public void computeHash(Hasher hasher) {
        super.computeHash(hasher);
        hasher.put(engine);
    }

    @Override
    public boolean compare(PgStatement obj) {
        if (this == obj) {
            return true;
        }
        return obj instanceof ChSchema schema && super.compare(schema)
                && compareUnalterable(schema);
    }

    private boolean compareUnalterable(ChSchema newSchema) {
        return Objects.equals(engine, newSchema.engine)
                && Objects.equals(comment, newSchema.comment);
    }

    @Override
    public boolean compareChildren(PgStatement obj) {
        if (obj instanceof ChSchema schema) {
            return super.compareChildren(obj)
                    && dictionaries.equals(schema.dictionaries);
        }
        return false;
    }

    @Override
    protected void computeChildrenHash(Hasher hasher) {
        super.computeChildrenHash(hasher);
        hasher.putUnordered(dictionaries);
    }

    @Override
    protected AbstractSchema getSchemaCopy() {
        var schema = new ChSchema(name);
        schema.setEngine(engine);
        return schema;
    }

    @Override
    public void appendComments(SQLScript script) {
        // no impl
    }
}
