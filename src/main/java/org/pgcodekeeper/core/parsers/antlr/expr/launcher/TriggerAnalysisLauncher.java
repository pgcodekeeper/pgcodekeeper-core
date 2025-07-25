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
import org.pgcodekeeper.core.parsers.antlr.expr.ValueExprWithNmspc;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.VexContext;
import org.pgcodekeeper.core.schema.PgObjLocation;
import org.pgcodekeeper.core.schema.meta.MetaContainer;
import org.pgcodekeeper.core.schema.pg.PgTrigger;

import java.util.Set;

/**
 * Launcher for analyzing PostgreSQL trigger conditions.
 * Handles the WHEN condition expressions in trigger definitions.
 */
public class TriggerAnalysisLauncher extends AbstractAnalysisLauncher {

    /**
     * Creates a trigger condition analyzer.
     *
     * @param stmt     the trigger statement being analyzed
     * @param ctx      the trigger condition expression context
     * @param location the source location identifier
     */
    public TriggerAnalysisLauncher(PgTrigger stmt, VexContext ctx, String location) {
        super(stmt, ctx, location);
    }

    @Override
    public Set<PgObjLocation> analyze(ParserRuleContext ctx, MetaContainer meta) {
        ValueExprWithNmspc vex = new ValueExprWithNmspc(meta);
        return analyzeTableChild((VexContext) ctx, vex);
    }
}
