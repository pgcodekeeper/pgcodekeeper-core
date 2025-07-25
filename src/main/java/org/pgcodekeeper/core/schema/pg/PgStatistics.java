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
 **
 * Copyright 2006 StartNet s.r.o.
 *
 * Distributed under MIT license
 *******************************************************************************/
package org.pgcodekeeper.core.schema.pg;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.pgcodekeeper.core.PgDiffUtils;
import org.pgcodekeeper.core.hashers.Hasher;
import org.pgcodekeeper.core.schema.AbstractStatistics;
import org.pgcodekeeper.core.schema.ObjectState;
import org.pgcodekeeper.core.schema.PgStatement;
import org.pgcodekeeper.core.schema.StatementUtils;
import org.pgcodekeeper.core.script.SQLScript;

public final class PgStatistics extends AbstractStatistics {

    private int statistics = -1;
    private List<String> kinds = new ArrayList<>();
    private List<String> expressions = new ArrayList<>();
    private String foreignSchema;
    private String foreignTable;

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
            sb.append(PgDiffUtils.getQuotedName(foreignSchema)).append('.');
        }
        sb.append(PgDiffUtils.getQuotedName(foreignTable));
        script.addStatement(sb);

        if (statistics > 0) {
            script.addStatement(appendStatistics(this));
        }

        appendOwnerSQL(script);
        appendComments(script);
    }

    @Override
    public ObjectState appendAlterSQL(PgStatement newCondition, SQLScript script) {
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

    public void addKind(String kind) {
        kinds.add(kind);
        resetHash();
    }

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
    public boolean compare(PgStatement obj) {
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
                && Objects.equals(expressions, stat.expressions)
                && Objects.equals(stat.foreignSchema, foreignSchema)
                && Objects.equals(stat.foreignTable, foreignTable);
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.put(kinds);
        hasher.put(expressions);
        hasher.put(statistics);
        hasher.put(foreignSchema);
        hasher.put(foreignTable);
    }

    @Override
    protected PgStatistics getStatisticsCopy() {
        PgStatistics stat = new PgStatistics(name);
        stat.kinds.addAll(kinds);
        stat.expressions.addAll(expressions);
        stat.setStatistics(statistics);
        stat.setForeignSchema(foreignSchema);
        stat.setForeignTable(foreignTable);
        return stat;
    }
}
