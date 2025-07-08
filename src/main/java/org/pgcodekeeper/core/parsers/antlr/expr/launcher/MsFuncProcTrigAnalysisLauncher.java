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
import java.util.Set;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.generated.TSQLParser.ExpressionContext;
import org.pgcodekeeper.core.parsers.antlr.generated.TSQLParser.Select_statementContext;
import org.pgcodekeeper.core.parsers.antlr.generated.TSQLParser.Sql_clausesContext;
import org.pgcodekeeper.core.parsers.antlr.msexpr.MsSelect;
import org.pgcodekeeper.core.parsers.antlr.msexpr.MsSqlClauses;
import org.pgcodekeeper.core.parsers.antlr.msexpr.MsValueExpr;
import org.pgcodekeeper.core.schema.PgObjLocation;
import org.pgcodekeeper.core.schema.meta.MetaContainer;
import org.pgcodekeeper.core.schema.ms.AbstractMsFunction;
import org.pgcodekeeper.core.schema.ms.MsTrigger;

public final class MsFuncProcTrigAnalysisLauncher extends AbstractAnalysisLauncher {

    private final boolean isEnableFunctionBodiesDependencies;

    public MsFuncProcTrigAnalysisLauncher(AbstractMsFunction stmt,
            Sql_clausesContext ctx, String location, boolean isEnableFunctionBodiesDependencies) {
        super(stmt, ctx, location);
        this.isEnableFunctionBodiesDependencies = isEnableFunctionBodiesDependencies;
    }

    public MsFuncProcTrigAnalysisLauncher(AbstractMsFunction stmt,
            Select_statementContext ctx, String location, boolean isEnableFunctionBodiesDependencies) {
        super(stmt, ctx, location);
        this.isEnableFunctionBodiesDependencies = isEnableFunctionBodiesDependencies;
    }

    public MsFuncProcTrigAnalysisLauncher(AbstractMsFunction stmt,
            ExpressionContext ctx, String location, boolean isEnableFunctionBodiesDependencies) {
        super(stmt, ctx, location);
        this.isEnableFunctionBodiesDependencies = isEnableFunctionBodiesDependencies;
    }

    public MsFuncProcTrigAnalysisLauncher(MsTrigger stmt,
            Sql_clausesContext ctx, String location, boolean isEnableFunctionBodiesDependencies) {
        super(stmt, ctx, location);
        this.isEnableFunctionBodiesDependencies = isEnableFunctionBodiesDependencies;
    }

    @Override
    public Set<PgObjLocation> analyze(ParserRuleContext ctx, MetaContainer meta) {
        String schema = getSchemaName();

        if (ctx instanceof Sql_clausesContext sqlCtx) {
            MsSqlClauses clauses = new MsSqlClauses(schema, meta);
            clauses.analyze(sqlCtx);
            return clauses.getDepcies();
        }

        if (ctx instanceof Select_statementContext selectCtx) {
            MsSelect select = new MsSelect(schema, meta);
            return analyze(selectCtx, select);
        }

        MsValueExpr expr = new MsValueExpr(schema, meta);
        return analyze((ExpressionContext) ctx, expr);
    }

    @Override
    protected EnumSet<DbObjType> getDisabledDepcies() {
        if (!isEnableFunctionBodiesDependencies) {
            return EnumSet.of(DbObjType.FUNCTION, DbObjType.PROCEDURE);
        }

        return super.getDisabledDepcies();
    }
}
