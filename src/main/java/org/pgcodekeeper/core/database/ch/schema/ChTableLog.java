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

import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.hasher.Hasher;

/**
 * Represents a ClickHouse Log family table that supports constraints.
 * Extends ChTable with the ability to add CHECK and ASSUME constraints.
 */
public final class ChTableLog extends ChTable {

    private final List<ChConstraint> constrs = new ArrayList<>();

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
            sb.append("\n\tCONSTRAINT ").append(getQuotedName(constr.getName())).append(' ')
                    .append(constr.getDefinition()).append(',');
        }
    }

    @Override
    public void addChild(IStatement st) {
        if (st.getStatementType() == DbObjType.CONSTRAINT) {
            constrs.add((ChConstraint) st);
            resetHash();
            return;
        }
        super.addChild(st);
    }

    @Override
    protected boolean isNotEmptyTable() {
        return super.isNotEmptyTable() || !constrs.isEmpty();
    }

    @Override
    public void computeHash(Hasher hasher) {
        super.computeHash(hasher);
        hasher.putOrdered(constrs);
    }

    @Override
    protected boolean compareTable(AbstractStatement obj) {
        return obj instanceof ChTableLog table
                && super.compareTable(table)
                && Objects.equals(constrs, table.constrs);
    }

    @Override
    protected ChTableLog getTableCopy() {
        var table = new ChTableLog(name);
        table.constrs.addAll(constrs);
        return table;
    }

    @Override
    protected boolean isNeedRecreate(ChTable newTable) {
        var newChLogTable = (ChTableLog) newTable;
        return super.isNeedRecreate(newChLogTable)
                || !constrs.equals(newChLogTable.constrs)
                || !columns.equals(newChLogTable.columns);
    }
}