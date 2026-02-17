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
import java.util.stream.Stream;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;
import org.pgcodekeeper.core.utils.Pair;

/**
 * Represents a ClickHouse dictionary object.
 * Dictionaries in ClickHouse are used for storing key-value data for fast lookups
 * and can have various sources and layouts.
 */
public class ChDictionary extends ChAbstractStatement implements IRelation {

    private final List<ChColumn> columns = new ArrayList<>();
    private final Map<String, String> sources = new LinkedHashMap<>();
    private final Map<String, String> options = new LinkedHashMap<>();

    private String sourceType;
    private String lifeTime;
    private String layOut;
    private String pk;
    private String range;

    /**
     * Creates a new ClickHouse dictionary with the specified name.
     *
     * @param name the name of the dictionary
     */
    public ChDictionary(String name) {
        super(name);
    }

    @Override
    public DbObjType getStatementType() {
        return DbObjType.DICTIONARY;
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        var sb = new StringBuilder();
        sb.append("CREATE DICTIONARY ");
        appendIfNotExists(sb, script.getSettings());
        sb.append(getQualifiedName());
        appendColumns(sb);
        if (pk != null) {
            sb.append("\nPRIMARY KEY ").append(pk);
        }
        if (!sources.isEmpty()) {
            sb.append("\nSOURCE(").append(sourceType).append('(');
            sources.forEach((k, v) -> sb.append(k).append(' ').append(v).append(' '));
            sb.setLength(sb.length() - 1);
            sb.append("))");
        }
        if (lifeTime != null) {
            sb.append("\nLIFETIME(").append(lifeTime).append(')');
        }
        if (layOut != null) {
            sb.append("\nLAYOUT(").append(layOut).append(')');
        }
        if (range != null) {
            sb.append("\nRANGE(").append(range).append(')');
        }

        if (!options.isEmpty()) {
            sb.append("\nSETTINGS(");
            options.forEach((k, v) -> sb.append(k).append(" = ").append(v).append(", "));
            sb.setLength(sb.length() - 2);
            sb.append(')');
        }

        if (getComment() != null) {
            sb.append("\nCOMMENT ").append(getComment());
        }
        script.addStatement(sb);
    }

    private void appendColumns(StringBuilder sb) {
        if (columns.isEmpty()) {
            return;
        }

        sb.append("\n(\n\t");
        for (var column : columns) {
            sb.append(column.getFullDefinition()).append(",\n\t");
        }
        sb.setLength(sb.length() - 3);
        sb.append("\n)");
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        if (!compare(newCondition)) {
            return ObjectState.RECREATE;
        }
        return ObjectState.NOTHING;
    }

    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
        resetHash();
    }

    public void setLifeTime(String lifeTime) {
        this.lifeTime = lifeTime;
        resetHash();
    }

    public void setLayOut(String layOut) {
        this.layOut = layOut;
        resetHash();
    }

    public void setPk(String pk) {
        this.pk = pk;
        resetHash();
    }

    public void setRange(String range) {
        this.range = range;
        resetHash();
    }

    /**
     * Adds a column to this dictionary.
     *
     * @param column the column to add
     */
    public void addColumn(final ChColumn column) {
        assertUnique(getColumn(column.getName()), column);
        columns.add(column);
        column.setParent(this);
        resetHash();
    }

    /**
     * Finds column according to specified column {@code name}.
     *
     * @param name name of the column to be searched
     * @return found column or null if no such column has been found
     */
    private ChColumn getColumn(final String name) {
        for (ChColumn column : columns) {
            if (column.getName().equals(name)) {
                return column;
            }
        }
        return null;
    }

    public void addSource(String key, String value) {
        sources.put(key, value);
        resetHash();
    }

    public void addOption(String option, String value) {
        options.put(option, value);
        resetHash();
    }

    @Override
    public Stream<Pair<String, String>> getRelationColumns() {
        return columns.stream().filter(c -> c.getType() != null)
                .map(c -> new Pair<>(c.getName(), c.getType()));
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.put(sourceType);
        hasher.put(lifeTime);
        hasher.put(layOut);
        hasher.put(pk);
        hasher.put(range);
        hasher.putOrdered(columns);
        hasher.put(sources);
        hasher.put(options);
    }

    @Override
    public boolean compare(IStatement obj) {
        if (this == obj) {
            return true;
        }
        return obj instanceof ChDictionary dictn && super.compare(dictn)
                && Objects.equals(sourceType, dictn.sourceType)
                && Objects.equals(lifeTime, dictn.lifeTime)
                && Objects.equals(layOut, dictn.layOut)
                && Objects.equals(pk, dictn.pk)
                && Objects.equals(range, dictn.range)
                && Objects.equals(columns, dictn.columns)
                && Objects.equals(sources, dictn.sources)
                && Objects.equals(options, dictn.options);
    }

    @Override
    protected AbstractStatement getCopy() {
        var copy = new ChDictionary(name);
        copy.setSourceType(sourceType);
        copy.setLifeTime(lifeTime);
        copy.setLayOut(layOut);
        copy.setPk(pk);
        copy.setRange(range);
        for (var colSrc : columns) {
            copy.addColumn((ChColumn) colSrc.deepCopy());
        }
        copy.sources.putAll(sources);
        copy.options.putAll(options);
        return copy;
    }
}
