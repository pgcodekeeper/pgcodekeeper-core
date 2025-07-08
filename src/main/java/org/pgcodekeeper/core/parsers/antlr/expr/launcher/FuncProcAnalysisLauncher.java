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

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.expr.AbstractExprWithNmspc;
import org.pgcodekeeper.core.parsers.antlr.expr.Function;
import org.pgcodekeeper.core.parsers.antlr.expr.Sql;
import org.pgcodekeeper.core.parsers.antlr.expr.SqlFunctionBody;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.Function_bodyContext;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.Plpgsql_functionContext;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.SqlContext;
import org.pgcodekeeper.core.schema.GenericColumn;
import org.pgcodekeeper.core.schema.PgObjLocation;
import org.pgcodekeeper.core.schema.meta.MetaContainer;
import org.pgcodekeeper.core.schema.pg.AbstractPgFunction;
import org.pgcodekeeper.core.utils.Pair;

public final class FuncProcAnalysisLauncher extends AbstractAnalysisLauncher {

    /**
     * Contains pairs, each of which contains the name of the function argument
     * and its full type name in GenericColumn object (typeSchema, typeName, DbObjType.TYPE).
     * Used to set up namespace for function body analysis.
     */
    private final List<Pair<String, GenericColumn>> funcArgs;
    private final boolean isEnableFunctionBodiesDependencies;

    public FuncProcAnalysisLauncher(AbstractPgFunction stmt, SqlContext ctx,
            String location, List<Pair<String, GenericColumn>> funcArgs, boolean isEnableFunctionBodiesDependencies) {
        super(stmt, ctx, location);
        this.funcArgs = funcArgs;
        this.isEnableFunctionBodiesDependencies = isEnableFunctionBodiesDependencies;
    }

    public FuncProcAnalysisLauncher(AbstractPgFunction stmt, Function_bodyContext ctx,
            String location, List<Pair<String, GenericColumn>> funcArgs, boolean isEnableFunctionBodiesDependencies) {
        super(stmt, ctx, location);
        this.funcArgs = funcArgs;
        this.isEnableFunctionBodiesDependencies = isEnableFunctionBodiesDependencies;
    }

    public FuncProcAnalysisLauncher(AbstractPgFunction stmt, Plpgsql_functionContext ctx,
            String location, List<Pair<String, GenericColumn>> funcArgs, boolean isEnableFunctionBodiesDependencies) {
        super(stmt, ctx, location);
        this.funcArgs = funcArgs;
        this.isEnableFunctionBodiesDependencies = isEnableFunctionBodiesDependencies;
    }

    @Override
    public Set<PgObjLocation> analyze(ParserRuleContext ctx, MetaContainer meta) {
        if (ctx instanceof SqlContext sqlCtx) {
            Sql sql = new Sql(meta);
            declareAnalyzerArgs(sql);
            return analyze(sqlCtx, sql);
        }

        if (ctx instanceof Function_bodyContext bodyCtx) {
            SqlFunctionBody body = new SqlFunctionBody(meta);
            declareAnalyzerArgs(body);
            return analyze(bodyCtx, body);
        }

        Function function = new Function(meta);
        declareAnalyzerArgs(function);
        return analyze((Plpgsql_functionContext) ctx, function);
    }

    private void declareAnalyzerArgs(AbstractExprWithNmspc<? extends ParserRuleContext> analyzer) {
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
