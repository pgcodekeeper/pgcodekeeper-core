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

import java.util.*;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.api.schema.meta.IMetaContainer;
import org.pgcodekeeper.core.database.pg.parser.expr.*;
import org.pgcodekeeper.core.database.pg.parser.generated.SQLParser.*;
import org.pgcodekeeper.core.database.pg.schema.PgAbstractFunction;
import org.pgcodekeeper.core.utils.Pair;

/**
 * Launcher for analyzing PostgreSQL function and procedure bodies.
 * Handles SQL, function body and PL/pgSQL function contexts with argument namespace support.
 */
public final class PgFuncProcAnalysisLauncher extends PgAbstractAnalysisLauncher {

    /**
     * Contains pairs, each of which contains the name of the function argument
     * and its full type name in GenericColumn object
     * Used to set up namespace for function body analysis.
     */
    private final List<Pair<String, GenericColumn>> funcArgs;
    private final boolean isEnableFunctionBodiesDependencies;

    /**
     * Creates a function/procedure analyzer for SQL context.
     *
     * @param stmt                               the function/procedure statement
     * @param ctx                                the SQL context to analyze
     * @param location                           the source location identifier
     * @param funcArgs                           list of function arguments
     * @param isEnableFunctionBodiesDependencies flag to control function body dependency collection
     */
    public PgFuncProcAnalysisLauncher(PgAbstractFunction stmt, SqlContext ctx,
            String location, List<Pair<String, GenericColumn>> funcArgs, boolean isEnableFunctionBodiesDependencies) {
        super(stmt, ctx, location);
        this.funcArgs = funcArgs;
        this.isEnableFunctionBodiesDependencies = isEnableFunctionBodiesDependencies;
    }

    /**
     * Creates a function/procedure analyzer for function body context.
     *
     * @param stmt                               the function/procedure statement
     * @param ctx                                the function body context to analyze
     * @param location                           the source location identifier
     * @param funcArgs                           list of function arguments
     * @param isEnableFunctionBodiesDependencies flag to control function body dependency collection
     */
    public PgFuncProcAnalysisLauncher(PgAbstractFunction stmt, Function_bodyContext ctx,
            String location, List<Pair<String, GenericColumn>> funcArgs, boolean isEnableFunctionBodiesDependencies) {
        super(stmt, ctx, location);
        this.funcArgs = funcArgs;
        this.isEnableFunctionBodiesDependencies = isEnableFunctionBodiesDependencies;
    }

    /**
     * Creates a function/procedure analyzer for PL/pgSQL context.
     *
     * @param stmt                               the function/procedure statement
     * @param ctx                                the PL/pgSQL function context to analyze
     * @param location                           the source location identifier
     * @param funcArgs                           list of function arguments
     * @param isEnableFunctionBodiesDependencies flag to control function body dependency collection
     */
    public PgFuncProcAnalysisLauncher(PgAbstractFunction stmt, Plpgsql_functionContext ctx,
            String location, List<Pair<String, GenericColumn>> funcArgs, boolean isEnableFunctionBodiesDependencies) {
        super(stmt, ctx, location);
        this.funcArgs = funcArgs;
        this.isEnableFunctionBodiesDependencies = isEnableFunctionBodiesDependencies;
    }

    @Override
    public Set<ObjectLocation> analyze(ParserRuleContext ctx, IMetaContainer meta) {
        if (ctx instanceof SqlContext sqlCtx) {
            PgSql sql = new PgSql(meta);
            declareAnalyzerArgs(sql);
            return analyze(sqlCtx, sql);
        }

        if (ctx instanceof Function_bodyContext bodyCtx) {
            PgSqlFunctionBody body = new PgSqlFunctionBody(meta);
            declareAnalyzerArgs(body);
            return analyze(bodyCtx, body);
        }

        PgFunctionExp function = new PgFunctionExp(meta);
        declareAnalyzerArgs(function);
        return analyze((Plpgsql_functionContext) ctx, function);
    }

    private void declareAnalyzerArgs(PgAbstractExprWithNmspc<? extends ParserRuleContext> analyzer) {
        for (int i = 0; i < funcArgs.size(); i++) {
            Pair<String, GenericColumn> arg = funcArgs.get(i);
            analyzer.declareNamespaceVar("$" + (i + 1), arg.getFirst(), arg.getSecond());
        }
    }

    @Override
    protected EnumSet<DbObjType> getDisabledDepcies() {
        if (!isEnableFunctionBodiesDependencies) {
            return EnumSet.of(DbObjType.FUNCTION, DbObjType.PROCEDURE);
        }

        return super.getDisabledDepcies();
    }
}
