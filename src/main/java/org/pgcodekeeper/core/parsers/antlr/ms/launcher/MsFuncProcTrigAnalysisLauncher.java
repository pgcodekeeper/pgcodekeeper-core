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
package org.pgcodekeeper.core.parsers.antlr.ms.launcher;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.base.launcher.AbstractAnalysisLauncher;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.ExpressionContext;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.Select_statementContext;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.Sql_clausesContext;
import org.pgcodekeeper.core.parsers.antlr.ms.expr.MsSelect;
import org.pgcodekeeper.core.parsers.antlr.ms.expr.MsSqlClauses;
import org.pgcodekeeper.core.parsers.antlr.ms.expr.MsValueExpr;
import org.pgcodekeeper.core.database.api.schema.ObjectLocation;
import org.pgcodekeeper.core.database.base.schema.meta.MetaContainer;
import org.pgcodekeeper.core.database.ms.schema.MsAbstractFunction;
import org.pgcodekeeper.core.database.ms.schema.MsTrigger;

import java.util.EnumSet;
import java.util.Set;

/**
 * Launcher for analyzing Microsoft SQL functions, procedures and triggers.
 * Handles SQL clauses, SELECT statements and expressions with dependency control.
 */
public final class MsFuncProcTrigAnalysisLauncher extends AbstractAnalysisLauncher {

    private final boolean isEnableFunctionBodiesDependencies;

    /**
     * Creates analyzer for SQL clauses in functions/procedures.
     *
     * @param stmt                               the function/procedure statement
     * @param ctx                                the SQL clauses context
     * @param location                           source location identifier
     * @param isEnableFunctionBodiesDependencies controls function body dependency collection
     */
    public MsFuncProcTrigAnalysisLauncher(MsAbstractFunction stmt,
            Sql_clausesContext ctx, String location, boolean isEnableFunctionBodiesDependencies) {
        super(stmt, ctx, location);
        this.isEnableFunctionBodiesDependencies = isEnableFunctionBodiesDependencies;
    }

    /**
     * Creates analyzer for SELECT statements in functions/procedures.
     *
     * @param stmt                               the function/procedure statement
     * @param ctx                                the SELECT statement context
     * @param location                           source location identifier
     * @param isEnableFunctionBodiesDependencies controls function body dependency collection
     */
    public MsFuncProcTrigAnalysisLauncher(MsAbstractFunction stmt,
            Select_statementContext ctx, String location, boolean isEnableFunctionBodiesDependencies) {
        super(stmt, ctx, location);
        this.isEnableFunctionBodiesDependencies = isEnableFunctionBodiesDependencies;
    }

    /**
     * Creates analyzer for expressions in functions/procedures.
     *
     * @param stmt                               the function/procedure statement
     * @param ctx                                the expression context
     * @param location                           source location identifier
     * @param isEnableFunctionBodiesDependencies controls function body dependency collection
     */
    public MsFuncProcTrigAnalysisLauncher(MsAbstractFunction stmt,
            ExpressionContext ctx, String location, boolean isEnableFunctionBodiesDependencies) {
        super(stmt, ctx, location);
        this.isEnableFunctionBodiesDependencies = isEnableFunctionBodiesDependencies;
    }

    /**
     * Creates analyzer for trigger SQL clauses.
     *
     * @param stmt                               the trigger statement
     * @param ctx                                the SQL clauses context
     * @param location                           source location identifier
     * @param isEnableFunctionBodiesDependencies controls function body dependency collection
     */
    public MsFuncProcTrigAnalysisLauncher(MsTrigger stmt,
            Sql_clausesContext ctx, String location, boolean isEnableFunctionBodiesDependencies) {
        super(stmt, ctx, location);
        this.isEnableFunctionBodiesDependencies = isEnableFunctionBodiesDependencies;
    }

    @Override
    public Set<ObjectLocation> analyze(ParserRuleContext ctx, MetaContainer meta) {
        String schema = getSchemaName();

        if (ctx instanceof Sql_clausesContext sqlCtx) {
            MsSqlClauses clauses = new MsSqlClauses(schema, meta);
            clauses.analyze(sqlCtx);
            return clauses.getDependencies();
        }

        if (ctx instanceof Select_statementContext selectCtx) {
            MsSelect select = new MsSelect(schema, meta);
            select.analyze(selectCtx);
            return select.getDependencies();
        }

        MsValueExpr expr = new MsValueExpr(schema, meta);
        expr.analyze((ExpressionContext) ctx);
        return expr.getDependencies();
    }

    @Override
    protected EnumSet<DbObjType> getDisabledDepcies() {
        if (!isEnableFunctionBodiesDependencies) {
            return EnumSet.of(DbObjType.FUNCTION, DbObjType.PROCEDURE);
        }

        return super.getDisabledDepcies();
    }
}
