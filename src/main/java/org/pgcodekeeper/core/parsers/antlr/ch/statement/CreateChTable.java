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
package org.pgcodekeeper.core.parsers.antlr.ch.statement;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.base.QNameParser;
import org.pgcodekeeper.core.parsers.antlr.ch.generated.CHParser.Create_table_stmtContext;
import org.pgcodekeeper.core.parsers.antlr.ch.generated.CHParser.IdentifierContext;
import org.pgcodekeeper.core.parsers.antlr.ch.generated.CHParser.Table_element_exprContext;
import org.pgcodekeeper.core.database.ch.schema.ChDatabase;
import org.pgcodekeeper.core.database.ch.schema.ChTable;
import org.pgcodekeeper.core.database.ch.schema.ChTableLog;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.List;

/**
 * Parser for ClickHouse CREATE TABLE statements.
 * Handles table creation including columns, engines, constraints, indexes, and projections.
 * Automatically selects appropriate table type (ChTable or ChTableLog) based on engine type.
 */
public final class CreateChTable extends ChParserAbstract {

    private final Create_table_stmtContext ctx;

    /**
     * Creates a parser for ClickHouse CREATE TABLE statements.
     *
     * @param ctx      the ANTLR parse tree context for the CREATE TABLE statement
     * @param db       the ClickHouse database schema being processed
     * @param settings parsing configuration settings
     */
    public CreateChTable(Create_table_stmtContext ctx, ChDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        List<ParserRuleContext> ids = getIdentifiers(ctx.qualified_name());
        String name = QNameParser.getFirstName(ids);
        ChTable table;
        IdentifierContext engType = null;
        var engineClCtx = ctx.table_body_expr().engine_clause();
        if (engineClCtx != null) {
            engType = engineClCtx.engine_expr().identifier();
        }
        if (engType != null && engType.getText().endsWith("Log")) {
            table = new ChTableLog(name);
        } else {
            table = new ChTable(name);
        }
        parseObject(table);
        addSafe(getSchemaSafe(ids), table, ids);
    }

    /**
     * Parses table details including engine, elements, and comments.
     * Processes all table elements such as columns, constraints, indexes, and projections.
     *
     * @param table the table object to populate with parsed information
     */
    public void parseObject(ChTable table) {
        table.setEngine(getEnginePart(ctx.table_body_expr().engine_clause()));
        for (Table_element_exprContext elementCtx : ctx.table_body_expr().table_element_expr()) {
            parseTableElement(table, elementCtx);
        }
        if (ctx.comment_expr() != null) {
            table.setComment(ctx.comment_expr().STRING_LITERAL().getText());
        }
    }

    private void parseTableElement(ChTable table, Table_element_exprContext elementCtx) {
        var columnCtx = elementCtx.table_column_def();
        if (columnCtx != null) {
            table.addColumn(getColumn(columnCtx));
            return;
        }
        var pkCtx = elementCtx.primary_key_clause();
        if (pkCtx != null) {
            table.setPkExpr(getFullCtxText(pkCtx.expr()));
            return;
        }
        var constrCtx = elementCtx.table_constraint_def();
        if (constrCtx != null) {
            table.addChild(getConstraint(elementCtx.table_constraint_def()));
            return;
        }
        var indexCtx = elementCtx.table_index_def();
        if (indexCtx != null) {
            table.addChild(getIndex(indexCtx));
            return;
        }
        var projCtx = elementCtx.table_projection_def();
        if (projCtx != null) {
            table.addProjection(projCtx.qualified_name().getText(),
                    '(' + getFullCtxText(projCtx.select_stmt_no_parens()) + ')');
            return;
        }

        throw new IllegalArgumentException("unsupported Table_element_exprContext\n" + getFullCtxText(elementCtx));
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_CREATE, DbObjType.TABLE, ctx.qualified_name());
    }
}
