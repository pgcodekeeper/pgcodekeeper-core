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
package org.pgcodekeeper.core.database.pg.schema;

import java.util.*;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;
import org.pgcodekeeper.core.utils.Utils;

/**
 * PostgreSQL extended statistics implementation.
 * Extended statistics collect additional information about column correlations
 * and distributions to improve query planning for multi-column predicates.
 */
public final class PgStatistics extends PgAbstractStatement implements IStatistics, ISearchPath {

    private int statistics = -1;
    private final List<String> kinds = new ArrayList<>();
    private final List<String> expressions = new ArrayList<>();
    private String foreignSchema;
    private String foreignTable;

    /**
     * Creates a new PostgreSQL statistics object.
     *
     * @param name statistics object name
     */
    public PgStatistics(String name) {
        super(name);
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        final StringBuilder sb = new StringBuilder();
        sb.append("CREATE STATISTICS ");
        appendIfNotExists(sb, script.getSettings());
        sb.append(getQualifiedName());
        StatementUtils.appendCollection(sb, kinds, ", ", true);
        sb.append(" ON ");
        StatementUtils.appendCollection(sb, expressions, ", ", false);
        sb.append(" FROM ");
        if (foreignSchema != null) {
            sb.append(quote(foreignSchema)).append('.');
        }
        sb.append(quote(foreignTable));
        script.addStatement(sb);

        if (statistics >= 0) {
            script.addStatement(appendStatistics(this));
        }

        appendOwnerSQL(script);
        appendComments(script);
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        int startSize = script.getSize();
        PgStatistics newStat = (PgStatistics) newCondition;
        if (!compareUnalterable(newStat)) {
            return ObjectState.RECREATE;
        }

        if (statistics != newStat.statistics) {
            script.addStatement(appendStatistics(newStat));
        }

        appendAlterOwner(newStat, script);
        appendAlterComments(newStat, script);

        return getObjectState(script, startSize);
    }

    private String appendStatistics(PgStatistics stat) {
        return "ALTER STATISTICS " + stat.getQualifiedName() + " SET STATISTICS " + stat.statistics;
    }

    public void setForeignSchema(String foreignSchema) {
        this.foreignSchema = foreignSchema;
        resetHash();
    }

    public void setForeignTable(String foreignTable) {
        this.foreignTable = foreignTable;
        resetHash();
    }

    public void setStatistics(int statistics) {
        this.statistics = statistics;
        resetHash();
    }

    /**
     * Adds a statistics kind (ndistinct, dependencies, mcv, etc.).
     *
     * @param kind statistics type to collect
     */
    public void addKind(String kind) {
        kinds.add(kind);
        resetHash();
    }

    /**
     * Adds a column expression for statistics collection.
     *
     * @param expression column name or expression
     */
    public void addExpr(String expression) {
        expressions.add(expression);
        resetHash();
    }

    public String getForeignSchema() {
        return foreignSchema;
    }

    public String getForeignTable() {
        return foreignTable;
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.put(kinds);
        hasher.put(expressions);
        hasher.put(foreignSchema);
        hasher.put(foreignTable);
        hasher.put(statistics);
    }

    @Override
    public boolean compare(IStatement obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof PgStatistics stat && super.compare(obj)) {
            return compareUnalterable(stat)
                    && stat.statistics == statistics;
        }

        return false;
    }

    private boolean compareUnalterable(PgStatistics stat) {
        return Objects.equals(kinds, stat.kinds)
                && Utils.setLikeEquals(expressions, stat.expressions)
                && Objects.equals(stat.foreignSchema, foreignSchema)
                && Objects.equals(stat.foreignTable, foreignTable);
    }

    @Override
    protected PgStatistics getCopy() {
        PgStatistics stat = new PgStatistics(name);
        stat.kinds.addAll(kinds);
        stat.expressions.addAll(expressions);
        stat.setForeignSchema(foreignSchema);
        stat.setForeignTable(foreignTable);
        stat.setStatistics(statistics);
        return stat;
    }
}
