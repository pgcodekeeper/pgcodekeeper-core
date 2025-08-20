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
import org.pgcodekeeper.core.parsers.antlr.pg.expr.TypesSetManually;
import org.pgcodekeeper.core.parsers.antlr.pg.expr.ValueExprWithNmspc;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.VexContext;
import org.pgcodekeeper.core.schema.PgObjLocation;
import org.pgcodekeeper.core.schema.meta.MetaContainer;
import org.pgcodekeeper.core.schema.pg.PgDomain;
import org.pgcodekeeper.core.utils.Pair;

import java.util.Set;

/**
 * Launcher for analyzing domain type constraints.
 * Handles validation expressions for domain types with special VALUE variable support.
 */
public class DomainAnalysisLauncher extends AbstractPgAnalysisLauncher {

    /**
     * Creates a domain analyzer.
     *
     * @param stmt     the domain statement to analyze
     * @param ctx      the domain constraint expression context
     * @param location the source location identifier
     */
    public DomainAnalysisLauncher(PgDomain stmt, VexContext ctx, String location) {
        super(stmt, ctx, location);
    }

    @Override
    public Set<PgObjLocation> analyze(ParserRuleContext ctx, MetaContainer meta) {
        ValueExprWithNmspc vex = new ValueExprWithNmspc(meta);
        vex.addNamespaceVariable(new Pair<>("VALUE", TypesSetManually.UNKNOWN));
        return analyze((VexContext) ctx, vex);
    }
}
