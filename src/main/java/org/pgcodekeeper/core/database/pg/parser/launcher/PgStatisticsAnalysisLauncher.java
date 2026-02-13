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
package org.pgcodekeeper.core.database.pg.parser.launcher;

import java.util.Set;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.api.schema.meta.IMetaContainer;
import org.pgcodekeeper.core.database.base.parser.launcher.AbstractAnalysisLauncher;
import org.pgcodekeeper.core.database.pg.parser.expr.PgValueExprWithNmspc;
import org.pgcodekeeper.core.database.pg.parser.generated.SQLParser.VexContext;
import org.pgcodekeeper.core.database.pg.parser.rulectx.PgVex;
import org.pgcodekeeper.core.database.pg.schema.PgStatistics;

/**
 * Launcher for analyzing PostgreSQL extended statistics expressions.
 * Handles dependencies in statistics expressions with proper table reference setup.
 */
public class PgStatisticsAnalysisLauncher extends AbstractAnalysisLauncher {

    /**
     * Creates a statistics analyzer for PostgreSQL.
     *
     * @param stmt     the statistics statement to analyze
     * @param ctx      the statistics expression context
     * @param location the source location identifier
     */
    public PgStatisticsAnalysisLauncher(PgStatistics stmt, VexContext ctx, String location) {
        super(stmt, ctx, location);
    }

    @Override
    public Set<ObjectLocation> analyze(ParserRuleContext ctx, IMetaContainer meta) {
        PgValueExprWithNmspc expr = new PgValueExprWithNmspc(meta);

        if (stmt instanceof PgStatistics stat) {
            expr.addRawTableReference(
                    new ObjectReference(stat.getForeignSchema(), stat.getForeignTable(), DbObjType.TABLE));
        }

        expr.analyze(new PgVex((VexContext) ctx));
        return expr.getDependencies();
    }
}
