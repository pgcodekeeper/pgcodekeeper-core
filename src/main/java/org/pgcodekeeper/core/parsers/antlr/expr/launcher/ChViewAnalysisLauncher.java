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
package org.pgcodekeeper.core.parsers.antlr.expr.launcher;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.parsers.antlr.chexpr.ChSelect;
import org.pgcodekeeper.core.parsers.antlr.generated.CHParser.Select_stmtContext;
import org.pgcodekeeper.core.parsers.antlr.generated.CHParser.Subquery_clauseContext;
import org.pgcodekeeper.core.schema.PgObjLocation;
import org.pgcodekeeper.core.schema.ch.ChView;
import org.pgcodekeeper.core.schema.meta.MetaContainer;

import java.util.Collections;
import java.util.Set;

/**
 * Launcher for analyzing ClickHouse view definitions.
 * Handles the extraction of dependencies from view subqueries.
 */
public class ChViewAnalysisLauncher extends AbstractAnalysisLauncher {

    /**
     * Creates a view analyzer for ClickHouse.
     *
     * @param stmt     the view statement to analyze
     * @param vQuery   the view subquery context
     * @param location the source location identifier
     */
    public ChViewAnalysisLauncher(ChView stmt, Subquery_clauseContext vQuery, String location) {
        super(stmt, vQuery, location);
    }

    @Override
    public Set<PgObjLocation> analyze(ParserRuleContext ctx, MetaContainer meta) {
        Subquery_clauseContext subQueryCtx = (Subquery_clauseContext) ctx;
        Select_stmtContext selectCtx = subQueryCtx.select_stmt();
        if (selectCtx != null) {
            ChSelect select = new ChSelect(getSchemaName(), meta);
            return analyze(selectCtx, select);
        }

        return Collections.emptySet();
    }
}
