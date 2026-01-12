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
package org.pgcodekeeper.core.parsers.antlr.pg.launcher;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.parsers.antlr.pg.expr.ValueExpr;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.VexContext;
import org.pgcodekeeper.core.parsers.antlr.pg.rulectx.Vex;
import org.pgcodekeeper.core.database.base.schema.AbstractColumn;
import org.pgcodekeeper.core.database.api.schema.ObjectLocation;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.database.base.schema.meta.MetaContainer;

import java.util.Set;

/**
 * Launcher for analyzing value expressions (VEX) in SQL definitions.
 * Handles both column expressions and standalone value expressions.
 */
public class VexAnalysisLauncher extends AbstractPgAnalysisLauncher {

    /**
     * Creates a value expression analyzer.
     *
     * @param stmt     the statement containing the expression
     * @param ctx      the value expression context to analyze
     * @param location the source location identifier
     */
    public VexAnalysisLauncher(AbstractStatement stmt, VexContext ctx, String location) {
        super(stmt, ctx, location);
    }

    @Override
    public Set<ObjectLocation> analyze(ParserRuleContext ctx, MetaContainer meta) {
        if (stmt instanceof AbstractColumn) {
            return analyzeTableChildVex((VexContext) ctx, meta);
        }

        ValueExpr expr = new ValueExpr(meta);
        expr.analyze(new Vex((VexContext) ctx));
        return expr.getDependencies();
    }
}
