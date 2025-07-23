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
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.chexpr.ChValueExpr;
import org.pgcodekeeper.core.parsers.antlr.generated.CHParser.ExprContext;
import org.pgcodekeeper.core.schema.PgObjLocation;
import org.pgcodekeeper.core.schema.ch.ChFunction;
import org.pgcodekeeper.core.schema.meta.MetaContainer;

import java.util.EnumSet;
import java.util.Set;

/**
 * Launcher for analyzing ClickHouse function bodies and dependencies.
 * Provides control over function body dependency collection.
 */
public final class ChFuncAnalysisLauncher extends AbstractAnalysisLauncher {

    private final boolean isEnableFunctionBodiesDependencies;

    /**
     * Creates a function analyzer for ClickHouse.
     *
     * @param st                                 the function statement to analyze
     * @param ctx                                the function body expression context
     * @param location                           the source location identifier
     * @param isEnableFunctionBodiesDependencies flag to control function body dependency collection
     */
    public ChFuncAnalysisLauncher(ChFunction st, ExprContext ctx, String location,
            boolean isEnableFunctionBodiesDependencies) {
        super(st, ctx, location);
        this.isEnableFunctionBodiesDependencies = isEnableFunctionBodiesDependencies;
    }

    @Override
    public Set<PgObjLocation> analyze(ParserRuleContext ctx, MetaContainer meta) {
        ChValueExpr analyzer = new ChValueExpr(meta);
        return analyze(((ExprContext) ctx), analyzer);
    }

    @Override
    protected EnumSet<DbObjType> getDisabledDepcies() {
        if (!isEnableFunctionBodiesDependencies) {
            return EnumSet.of(DbObjType.FUNCTION);
        }

        return super.getDisabledDepcies();
    }
}
