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
package org.pgcodekeeper.core.parsers.antlr.statements.ch;

import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.generated.CHParser.Create_database_stmtContext;
import org.pgcodekeeper.core.parsers.antlr.generated.CHParser.Engine_exprContext;
import org.pgcodekeeper.core.schema.ch.ChDatabase;
import org.pgcodekeeper.core.schema.ch.ChSchema;
import org.pgcodekeeper.core.settings.ISettings;

/**
 * Parser for ClickHouse CREATE DATABASE statements.
 * Handles database (schema) creation including engine specifications and comments.
 */
public final class CreateChSchema extends ChParserAbstract {

    private final Create_database_stmtContext ctx;

    /**
     * Creates a parser for ClickHouse CREATE DATABASE statements.
     *
     * @param ctx      the ANTLR parse tree context for the CREATE DATABASE statement
     * @param db       the ClickHouse database schema being processed
     * @param settings parsing configuration settings
     */
    public CreateChSchema(Create_database_stmtContext ctx, ChDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        ChSchema schema = (ChSchema) createAndAddSchemaWithCheck(ctx.name_with_cluster().identifier());

        var engine = ctx.engine_expr();
        if (engine != null) {
            schema.setEngine(getEngine(engine));
        }
        var commCtx = ctx.comment_expr();
        if (commCtx != null) {
            schema.setComment(commCtx.STRING_LITERAL().getText());
        }
    }

    private String getEngine(Engine_exprContext engine) {
        var engineBody = new StringBuilder();
        engineBody.append(engine.identifier().getText());
        var exprList = engine.expr_list();
        if (exprList != null) {
            engineBody.append("(");
            for (var expr : exprList.expr()) {
                engineBody.append(getFullCtxText(expr)).append(", ");
            }
            engineBody.setLength(engineBody.length() - 2);
            engineBody.append(')');
        }
        return engineBody.toString();
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_CREATE, DbObjType.DATABASE, ctx.name_with_cluster().identifier());
    }
}