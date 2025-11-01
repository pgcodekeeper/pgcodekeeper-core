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

import org.pgcodekeeper.core.ChDiffUtils;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.schema.AbstractConstraint;
import org.pgcodekeeper.core.schema.AbstractTable;
import org.pgcodekeeper.core.schema.ObjectState;
import org.pgcodekeeper.core.schema.PgStatement;
import org.pgcodekeeper.core.script.SQLScript;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Represents a ClickHouse Log family table that supports constraints.
 * Extends ChTable with the ability to add CHECK and ASSUME constraints.
 */
public final class ChTableLog extends ChTable {

    private final List<AbstractConstraint> constrs = new ArrayList<>();

    /**
     * Creates a new ClickHouse Log table with the specified name.
     *
     * @param name the name of the table
     */
    public ChTableLog(String name) {
        super(name);
    }

    @Override
    protected void appendTableBody(StringBuilder sb) {
        super.appendTableBody(sb);
        for (var constr : constrs) {
            sb.append("\n\tCONSTRAINT ").append(ChDiffUtils.getQuotedName(constr.getName())).append(' ')
                    .append(constr.getDefinition()).append(',');
        }
    }

    @Override
    public void addConstraint(AbstractConstraint constraint) {
        constrs.add(constraint);
        resetHash();
    }

    @Override
    public ObjectState appendAlterSQL(PgStatement newCondition, SQLScript script) {
        return (compare(newCondition) && compareChildren(newCondition)) ? ObjectState.NOTHING : ObjectState.RECREATE;
    }

    @Override
    protected boolean isNotEmptyTable() {
        return super.isNotEmptyTable() || !constrs.isEmpty();
    }

    @Override
    protected boolean compareTable(PgStatement obj) {
        return obj instanceof ChTableLog table
                && super.compareTable(table)
                && Objects.equals(constrs, table.constrs);
    }

    @Override
    public void computeHash(Hasher hasher) {
        super.computeHash(hasher);
        hasher.putOrdered(constrs);
    }

    @Override
    protected AbstractTable getTableCopy() {
        var table = new ChTableLog(name);
        table.projections.putAll(projections);
        table.engine = engine;
        table.constrs.addAll(constrs);
        return table;
    }
}