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
package org.pgcodekeeper.core.parsers.antlr.ch.launcher;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.base.launcher.AbstractAnalysisLauncher;
import org.pgcodekeeper.core.parsers.antlr.ch.expr.ChExprWithNmspc;
import org.pgcodekeeper.core.parsers.antlr.ch.expr.ChValueExpr;
import org.pgcodekeeper.core.parsers.antlr.ch.generated.CHParser.ExprContext;
import org.pgcodekeeper.core.schema.GenericColumn;
import org.pgcodekeeper.core.schema.PgObjLocation;
import org.pgcodekeeper.core.schema.PgStatement;
import org.pgcodekeeper.core.schema.meta.MetaContainer;

import java.util.Set;

/**
 * Launcher for analyzing ClickHouse SQL expressions.
 * Handles both standalone expressions and expressions with namespace requirements.
 */
public class ChExpressionAnalysisLauncher extends AbstractAnalysisLauncher {

    /**
     * Creates an expression analyzer for ClickHouse SQL.
     *
     * @param stmt     the statement containing the expression
     * @param ctx      the expression parse tree context
     * @param location the source location identifier
     */
    public ChExpressionAnalysisLauncher(PgStatement stmt, ExprContext ctx, String location) {
        super(stmt, ctx, location);
    }

    @Override
    protected Set<PgObjLocation> analyze(ParserRuleContext ctx, MetaContainer meta) {
        if (isNeedNmspc()) {
            var expr = new ChExprWithNmspc(getSchemaName(), meta);
            PgStatement table = stmt.getParent();
            String schemaName = table.getParent().getName();
            String rawTableReference = table.getName();

            expr.addRawTableReference(new GenericColumn(schemaName, rawTableReference, DbObjType.TABLE));
            expr.analyze((ExprContext) ctx);
            return expr.getDependencies();
        }
        var expr = new ChValueExpr(meta);
        expr.analyze((ExprContext) ctx);
        return expr.getDependencies();
    }

    private boolean isNeedNmspc() {
        return stmt.isSubElement();
    }
}
