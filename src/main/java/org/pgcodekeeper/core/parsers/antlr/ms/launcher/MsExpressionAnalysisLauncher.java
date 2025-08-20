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
package org.pgcodekeeper.core.parsers.antlr.ms.launcher;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.base.launcher.AbstractAnalysisLauncher;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.ExpressionContext;
import org.pgcodekeeper.core.parsers.antlr.ms.expr.MsExprWithNmspc;
import org.pgcodekeeper.core.parsers.antlr.ms.expr.MsValueExpr;
import org.pgcodekeeper.core.schema.AbstractColumn;
import org.pgcodekeeper.core.schema.GenericColumn;
import org.pgcodekeeper.core.schema.PgObjLocation;
import org.pgcodekeeper.core.schema.PgStatement;
import org.pgcodekeeper.core.schema.meta.MetaContainer;

import java.util.Set;

/**
 * Launcher for analyzing Microsoft SQL expressions.
 * Handles both column expressions and standalone expressions with proper namespace management.
 */
public class MsExpressionAnalysisLauncher extends AbstractAnalysisLauncher {

    /**
     * Creates a Microsoft SQL expression analyzer.
     *
     * @param stmt     the statement containing the expression
     * @param ctx      the expression parse tree context
     * @param location the source location identifier
     */
    public MsExpressionAnalysisLauncher(PgStatement stmt, ExpressionContext ctx, String location) {
        super(stmt, ctx, location);
    }

    @Override
    public Set<PgObjLocation> analyze(ParserRuleContext ctx, MetaContainer meta) {
        if (stmt instanceof AbstractColumn) {
            var expr = new MsExprWithNmspc(getSchemaName(), meta);
            PgStatement table = stmt.getParent();
            String schemaName = table.getParent().getName();
            String rawTableReference = table.getName();

            expr.addRawTableReference(new GenericColumn(schemaName, rawTableReference, DbObjType.TABLE));
            expr.analyze((ExpressionContext) ctx);
            return expr.getDepcies();
        }
        MsValueExpr expr = new MsValueExpr(getSchemaName(), meta);
        expr.analyze((ExpressionContext) ctx);
        return expr.getDepcies();
    }
}