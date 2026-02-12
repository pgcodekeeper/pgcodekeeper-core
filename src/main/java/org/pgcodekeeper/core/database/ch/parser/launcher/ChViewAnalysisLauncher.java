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
package org.pgcodekeeper.core.database.ch.parser.launcher;

import java.util.*;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.database.api.schema.ObjectLocation;
import org.pgcodekeeper.core.database.api.schema.meta.IMetaContainer;
import org.pgcodekeeper.core.database.base.parser.launcher.AbstractAnalysisLauncher;
import org.pgcodekeeper.core.database.ch.parser.expr.ChSelect;
import org.pgcodekeeper.core.database.ch.parser.generated.CHParser.*;
import org.pgcodekeeper.core.database.ch.schema.ChView;

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
    public Set<ObjectLocation> analyze(ParserRuleContext ctx, IMetaContainer meta) {
        Subquery_clauseContext subQueryCtx = (Subquery_clauseContext) ctx;
        Select_stmtContext selectCtx = subQueryCtx.select_stmt();
        if (selectCtx != null) {
            ChSelect select = new ChSelect(getSchemaName(), meta);
            select.analyze(selectCtx);
            return select.getDependencies();
        }

        return Collections.emptySet();
    }
}
