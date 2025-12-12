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
package org.pgcodekeeper.core.parsers.antlr.ch.statement;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.database.api.schema.DatabaseType;
import org.pgcodekeeper.core.formatter.FileFormatter;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.base.AntlrUtils;
import org.pgcodekeeper.core.parsers.antlr.base.QNameParser;
import org.pgcodekeeper.core.parsers.antlr.ch.launcher.ChViewAnalysisLauncher;
import org.pgcodekeeper.core.parsers.antlr.ch.generated.CHParser.*;
import org.pgcodekeeper.core.database.ch.schema.ChDatabase;
import org.pgcodekeeper.core.database.ch.schema.ChView;
import org.pgcodekeeper.core.database.ch.schema.ChView.ChViewType;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.List;
import java.util.Locale;

/**
 * Parser for ClickHouse CREATE VIEW statements.
 * Handles creation of simple views, materialized views, and live views with support for
 * columns, engines, destinations, definers, and SQL security settings.
 */
public final class CreateChView extends ChParserAbstract {

    private final Create_view_stmtContext ctx;
    private final CommonTokenStream stream;

    /**
     * Creates a parser for ClickHouse CREATE VIEW statements.
     *
     * @param ctx      the ANTLR parse tree context for the CREATE VIEW statement
     * @param db       the ClickHouse database schema being processed
     * @param stream   the token stream for whitespace normalization
     * @param settings parsing configuration settings
     */
    public CreateChView(Create_view_stmtContext ctx, ChDatabase db, CommonTokenStream stream, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
        this.stream = stream;
    }

    private Qualified_nameContext getQname() {
        var simpleViewCtx = ctx.create_simple_view_stmt();
        if (simpleViewCtx != null) {
            return simpleViewCtx.qualified_name();
        }

        var matViewCtx = ctx.create_mat_view_stmt();
        if (matViewCtx != null) {
            return matViewCtx.qualified_name();
        }

        return ctx.create_live_view_stmt().qualified_name();
    }

    @Override
    public void parseObject() {
        List<ParserRuleContext> ids = getIdentifiers(getQname());
        String name = QNameParser.getFirstName(ids);
        ChView view = new ChView(name);
        parseObject(view, false);
        addSafe(getSchemaSafe(ids), view, ids);
    }

    /**
     * Parses view details including query, comments, and view type-specific configurations.
     * Handles simple views, materialized views, and live views with their respective options.
     *
     * @param view the view object to populate with parsed information
     * @param needFormatSql if true format select part.
     */
    public void parseObject(ChView view, boolean needFormatSql) {
        var vQuery = ctx.subquery_clause();
        if (vQuery != null) {
            view.setQuery(getQuery(vQuery, needFormatSql), AntlrUtils.normalizeWhitespaceUnquoted(vQuery, stream, getDbType()));
            db.addAnalysisLauncher(new ChViewAnalysisLauncher(view, vQuery, fileName));
        }

        var commentCtx = ctx.comment_expr();
        if (commentCtx != null) {
            view.setComment(commentCtx.STRING_LITERAL().getText());
        }

        var simpleViewCtx = ctx.create_simple_view_stmt();
        if (simpleViewCtx != null) {
            parseSimpleView(simpleViewCtx, view);
            return;
        }
        Create_mat_view_stmtContext matViewCtx = ctx.create_mat_view_stmt();
        if ((matViewCtx) != null) {
            parseMatView(matViewCtx, view);
            return;
        }

        parseLiveView(ctx.create_live_view_stmt(), view);
    }

    private String getQuery(Subquery_clauseContext vQuery, boolean needFormatSql) {
        String sql = getFullCtxText(vQuery);
        if (needFormatSql && null != vQuery.select_stmt()) {
            return FileFormatter.formatSql(sql, DatabaseType.CH);
        }
        return sql;
    }

    private void parseSimpleView(Create_simple_view_stmtContext simpleViewCtx, ChView view) {
        view.setType(ChViewType.SIMPLE);
        fillColumns(simpleViewCtx.table_schema_clause(), view);

        setDefiner(simpleViewCtx.definer_clause(), view);
        setSqlSecurity(simpleViewCtx.sql_security_clause(), view);
    }

    private void parseLiveView(Create_live_view_stmtContext liveViewStmtCtx, ChView view) {
        view.setType(ChViewType.LIVE);
        if (liveViewStmtCtx.REFRESH() != null) {
            view.setWithRefresh(true);

            var period = liveViewStmtCtx.NUMBER();
            if (period != null) {
                view.setRefreshPeriod(Integer.parseInt(period.getText()));
            }
        }
        fillColumns(liveViewStmtCtx.table_schema_clause(), view);
    }

    private void parseMatView(Create_mat_view_stmtContext matViewCtx, ChView view) {
        view.setType(ChViewType.MATERIALIZED);
        fillColumns(matViewCtx.table_schema_clause(), view);

        var destCtx = matViewCtx.destination_clause();
        if (destCtx != null) {
            var qnameCtx = destCtx.qualified_name();
            var ids = getIdentifiers(qnameCtx);
            addDepSafe(view, ids, DbObjType.TABLE);
            view.setDestination(getFullCtxText(qnameCtx));
        }

        view.setEngine(getEnginePart(matViewCtx.engine_clause()));

        setDefiner(matViewCtx.definer_clause(), view);
        setSqlSecurity(matViewCtx.sql_security_clause(), view);
    }

    private void fillColumns(Table_schema_clauseContext tableSchemaCtx, ChView view) {
        if (tableSchemaCtx == null) {
            return;
        }

        for (var tableElementCtx : tableSchemaCtx.table_element_expr()) {
            Table_column_defContext columnCtx = tableElementCtx.table_column_def();
            if (columnCtx != null) {
                view.addColumn(getColumn(columnCtx));
            }
        }
    }

    private void setDefiner(Definer_clauseContext definerClauseCtx, ChView view) {
        if (definerClauseCtx == null) {
            return;
        }

        view.setDefiner(definerClauseCtx.identifier().getText());
    }

    private void setSqlSecurity(Sql_security_clauseContext sqlSecurityCtx, ChView view) {
        if (sqlSecurityCtx == null) {
            return;
        }

        view.setSqlSecurity(sqlSecurityCtx.sec.getText().toUpperCase(Locale.ROOT));
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_CREATE, DbObjType.VIEW, getQname());
    }
}
