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
package org.pgcodekeeper.core.database.ch.schema;

import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.database.api.schema.ObjectState;
import org.pgcodekeeper.core.database.base.schema.AbstractIndex;
import org.pgcodekeeper.core.database.base.schema.AbstractTable;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;

import java.util.Objects;

/**
 * Represents a ClickHouse table index.
 * ClickHouse indexes are used for data skipping and include expression, type, and granularity settings.
 */
public final class ChIndex extends AbstractIndex implements IChStatement {

    private String expr;
    private String type;
    private int granVal = 1;

    /**
     * Creates a new ClickHouse index with the specified name.
     *
     * @param name the name of the index
     */
    public ChIndex(String name) {
        super(name);
    }

    public void setExpr(String expr) {
        this.expr = expr;
        resetHash();
    }

    public void setType(String type) {
        this.type = type;
        resetHash();
    }

    public void setGranVal(int granVal) {
        this.granVal = granVal;
        resetHash();
    }

    /**
     * Returns the full definition string for this index.
     *
     * @return the complete index definition
     */
    public String getDefinition() {
        final StringBuilder sb = new StringBuilder();
        sb.append("INDEX ").append(name).append(' ').append(expr)
                .append(" TYPE ").append(type);
        if (granVal != 1) {
            sb.append(" GRANULARITY ").append(granVal);
        }
        return sb.toString();
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        script.addStatement(getAlterTable() + " ADD " + getDefinition());
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        var newIndex = (ChIndex) newCondition;
        if (!compareUnalterable(newIndex)) {
            return ObjectState.RECREATE;
        }
        return ObjectState.NOTHING;
    }

    @Override
    public void getDropSQL(SQLScript script, boolean optionExists) {
        final StringBuilder sb = new StringBuilder();
        sb.append(getAlterTable()).append("\n\tDROP INDEX ");
        if (optionExists) {
            sb.append(IF_EXISTS);
        }
        sb.append(getQuotedName(name));
        script.addStatement(sb);
    }

    private String getAlterTable() {
        return ((AbstractTable) parent).getAlterTable(false);
    }

    private boolean compareUnalterable(ChIndex newIndex) {
        return Objects.equals(expr, newIndex.expr)
                && Objects.equals(type, newIndex.type)
                && granVal == newIndex.granVal;
    }

    @Override
    public void computeHash(Hasher hasher) {
        super.computeHash(hasher);
        hasher.put(expr);
        hasher.put(type);
        hasher.put(granVal);
    }

    @Override
    public boolean compare(IStatement obj) {
        if (this == obj) {
            return true;
        }
        return obj instanceof ChIndex index && super.compare(index)
                && compareUnalterable(index);
    }

    @Override
    protected AbstractIndex getIndexCopy() {
        var index = new ChIndex(name);
        index.setExpr(expr);
        index.setType(type);
        index.setGranVal(granVal);
        return index;
    }
}