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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import org.pgcodekeeper.core.DatabaseType;
import org.pgcodekeeper.core.hashers.Hasher;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.schema.AbstractColumn;
import org.pgcodekeeper.core.schema.IRelation;
import org.pgcodekeeper.core.schema.ISchema;
import org.pgcodekeeper.core.schema.ObjectState;
import org.pgcodekeeper.core.schema.PgStatement;
import org.pgcodekeeper.core.script.SQLScript;
import org.pgcodekeeper.core.utils.Pair;

public final class ChDictionary extends PgStatement implements IRelation {

    private String sourceType;
    private String lifeTime;
    private String layOut;
    private String pk;
    private String range;
    private final List<AbstractColumn> columns = new ArrayList<>();
    private final Map<String, String> sources = new LinkedHashMap<>();
    private final Map<String, String> options = new LinkedHashMap<>();

    public ChDictionary(String name) {
        super(name);
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

    public void addColumn(final AbstractColumn column) {
        assertUnique(getColumn(column.getName()), column);
        columns.add(column);
        column.setParent(this);
        resetHash();
    }

    /**
     * Finds column according to specified column {@code name}.
     *
     * @param name name of the column to be searched
     *
     * @return found column or null if no such column has been found
     */
    private AbstractColumn getColumn(final String name) {
        for (AbstractColumn column : columns) {
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
    public ObjectState appendAlterSQL(PgStatement newCondition, SQLScript script) {
        if (!compare(newCondition)) {
            return ObjectState.RECREATE;
        }
        return ObjectState.NOTHING;
    }

    @Override
    public DatabaseType getDbType() {
        return DatabaseType.CH;
    }

    @Override
    public DbObjType getStatementType() {
        return DbObjType.DICTIONARY;
    }

    @Override
    public ISchema getContainingSchema() {
        return (ChSchema) parent;
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
    public boolean compare(PgStatement obj) {
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
    public void appendComments(SQLScript script) {
        // no impl
    }

    @Override
    protected void appendCommentSql(SQLScript script) {
        // no impl
    }

    @Override
    public PgStatement shallowCopy() {
        var dictn = new ChDictionary(name);
        copyBaseFields(dictn);
        dictn.setSourceType(sourceType);
        dictn.setLifeTime(lifeTime);
        dictn.setLayOut(layOut);
        dictn.setPk(pk);
        dictn.setRange(range);
        dictn.sources.putAll(sources);
        dictn.columns.addAll(columns);
        dictn.options.putAll(options);
        return dictn;
    }
}
