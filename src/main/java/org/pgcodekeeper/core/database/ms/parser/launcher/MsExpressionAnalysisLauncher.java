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
package org.pgcodekeeper.core.database.ms.parser.launcher;

import java.util.Set;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.api.schema.meta.IMetaContainer;
import org.pgcodekeeper.core.database.base.parser.launcher.AbstractAnalysisLauncher;
import org.pgcodekeeper.core.database.ms.parser.expr.*;
import org.pgcodekeeper.core.database.ms.parser.generated.TSQLParser.ExpressionContext;
import org.pgcodekeeper.core.database.ms.schema.MsAbstractStatement;

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
    public MsExpressionAnalysisLauncher(MsAbstractStatement stmt, ExpressionContext ctx, String location) {
        super(stmt, ctx, location);
    }

    @Override
    public Set<ObjectLocation> analyze(ParserRuleContext ctx, IMetaContainer meta) {
        if (stmt instanceof IColumn) {
            var expr = new MsExprWithNmspc(getSchemaName(), meta);
            var table = stmt.getParent();
            String schemaName = table.getParent().getName();
            String rawTableReference = table.getName();

            expr.addRawTableReference(new ObjectReference(schemaName, rawTableReference, DbObjType.TABLE));
            expr.analyze((ExpressionContext) ctx);
            return expr.getDependencies();
        }
        MsValueExpr expr = new MsValueExpr(getSchemaName(), meta);
        expr.analyze((ExpressionContext) ctx);
        return expr.getDependencies();
    }
}