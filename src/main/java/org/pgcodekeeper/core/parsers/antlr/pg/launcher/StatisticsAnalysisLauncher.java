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
package org.pgcodekeeper.core.parsers.antlr.pg.launcher;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.base.launcher.AbstractAnalysisLauncher;
import org.pgcodekeeper.core.parsers.antlr.pg.expr.ValueExprWithNmspc;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.VexContext;
import org.pgcodekeeper.core.parsers.antlr.pg.rulectx.Vex;
import org.pgcodekeeper.core.schema.GenericColumn;
import org.pgcodekeeper.core.schema.PgObjLocation;
import org.pgcodekeeper.core.schema.meta.MetaContainer;
import org.pgcodekeeper.core.schema.pg.PgStatistics;

import java.util.Set;

/**
 * Launcher for analyzing PostgreSQL extended statistics expressions.
 * Handles dependencies in statistics expressions with proper table reference setup.
 */
public class StatisticsAnalysisLauncher extends AbstractAnalysisLauncher {

    /**
     * Creates a statistics analyzer for PostgreSQL.
     *
     * @param stmt     the statistics statement to analyze
     * @param ctx      the statistics expression context
     * @param location the source location identifier
     */
    public StatisticsAnalysisLauncher(PgStatistics stmt, VexContext ctx, String location) {
        super(stmt, ctx, location);
    }

    @Override
    public Set<PgObjLocation> analyze(ParserRuleContext ctx, MetaContainer meta) {
        ValueExprWithNmspc expr = new ValueExprWithNmspc(meta);

        if (stmt instanceof PgStatistics stat) {
            expr.addRawTableReference(
                    new GenericColumn(stat.getForeignSchema(), stat.getForeignTable(), DbObjType.TABLE));
        }

        expr.analyze(new Vex((VexContext) ctx));
        return expr.getDependencies();
    }
}
