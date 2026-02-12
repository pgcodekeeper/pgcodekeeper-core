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
import org.pgcodekeeper.core.database.api.schema.ObjectLocation;
import org.pgcodekeeper.core.database.api.schema.meta.IMetaContainer;
import org.pgcodekeeper.core.database.pg.parser.expr.PgValueExprWithNmspc;
import org.pgcodekeeper.core.database.pg.parser.generated.SQLParser.VexContext;
import org.pgcodekeeper.core.database.pg.schema.PgTrigger;

/**
 * Launcher for analyzing PostgreSQL trigger conditions.
 * Handles the WHEN condition expressions in trigger definitions.
 */
public class PgTriggerAnalysisLauncher extends PgAbstractAnalysisLauncher {

    /**
     * Creates a trigger condition analyzer.
     *
     * @param stmt     the trigger statement being analyzed
     * @param ctx      the trigger condition expression context
     * @param location the source location identifier
     */
    public PgTriggerAnalysisLauncher(PgTrigger stmt, VexContext ctx, String location) {
        super(stmt, ctx, location);
    }

    @Override
    public Set<ObjectLocation> analyze(ParserRuleContext ctx, IMetaContainer meta) {
        PgValueExprWithNmspc vex = new PgValueExprWithNmspc(meta);
        return analyzeTableChild((VexContext) ctx, vex);
    }
}
