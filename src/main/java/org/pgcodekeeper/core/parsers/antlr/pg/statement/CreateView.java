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
package org.pgcodekeeper.core.parsers.antlr.pg.statement;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.base.AntlrParser;
import org.pgcodekeeper.core.parsers.antlr.base.AntlrUtils;
import org.pgcodekeeper.core.parsers.antlr.base.QNameParser;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.*;
import org.pgcodekeeper.core.parsers.antlr.pg.launcher.ViewAnalysisLauncher;
import org.pgcodekeeper.core.database.pg.schema.PgAbstractView;
import org.pgcodekeeper.core.database.pg.schema.PgMaterializedView;
import org.pgcodekeeper.core.database.pg.schema.PgDatabase;
import org.pgcodekeeper.core.database.pg.schema.PgView;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.List;

/**
 * Parser for PostgreSQL CREATE VIEW and CREATE MATERIALIZED VIEW statements.
 * <p>
 * This class handles parsing of view definitions including regular views,
 * materialized views, recursive views, and their associated properties such as
 * column names, storage parameters, tablespace, and check options.
 */
public final class CreateView extends PgParserAbstract {

    /**
     * Pattern for transforming recursive view definitions into standard form.
     */
    public static final String RECURSIVE_PATTERN = """
            CREATE VIEW %1$s
            AS WITH RECURSIVE %1$s(%2$s) AS (
            %3$s
            )
            SELECT %2$s
            FROM %1$s;""";

    private final Create_view_statementContext context;
    private final String tablespace;
    private final String accessMethod;
    private final CommonTokenStream stream;

    /**
     * Constructs a new CreateView parser.
     *
     * @param context      the CREATE VIEW statement context
     * @param db           the PostgreSQL database object
     * @param tablespace   the default tablespace name
     * @param accessMethod the default access method
     * @param stream       the token stream for parsing
     * @param settings     the ISettings object
     */
    public CreateView(Create_view_statementContext context, PgDatabase db,
                      String tablespace, String accessMethod, CommonTokenStream stream, ISettings settings) {
        super(db, settings);
        this.context = context;
        this.tablespace = tablespace;
        this.accessMethod = accessMethod;
        this.stream = stream;
    }

    @Override
    public void parseObject() {
        Create_view_statementContext ctx = context;
        List<ParserRuleContext> ids = getIdentifiers(ctx.name);
        ParserRuleContext name = QNameParser.getFirstNameCtx(ids);
        PgAbstractView view = new PgView(name.getText());
        if (ctx.MATERIALIZED() != null) {
            var matV = new PgMaterializedView(name.getText());
            matV.setIsWithData(ctx.NO() == null);
            Table_spaceContext space = ctx.table_space();
            if (space != null) {
                matV.setTablespace(space.identifier().getText());
            } else if (tablespace != null) {
                matV.setTablespace(tablespace);
            }
            if (ctx.USING() != null) {
                matV.setMethod(ctx.identifier().getText());
            } else if (accessMethod != null) {
                matV.setMethod(accessMethod);
            }
            if (ctx.distributed_clause() != null) {
                matV.setDistribution(parseDistribution(ctx.distributed_clause()));
            }
            view = matV;
        } else if (ctx.RECURSIVE() != null) {
            String sql = RECURSIVE_PATTERN.formatted(
                    getFullCtxText(name), getFullCtxText(ctx.column_names.identifier()), getFullCtxText(ctx.v_query));

            var parser = AntlrParser.createSQLParser(sql, "recursive view", null);
            ctx = parser.sql().statement(0).schema_statement().schema_create().create_view_statement();
        }
        Select_stmtContext vQuery = ctx.v_query;
        if (vQuery != null) {
            view.setQuery(getFullCtxText(vQuery), AntlrUtils.normalizeWhitespaceUnquoted(vQuery, stream));
            db.addAnalysisLauncher(new ViewAnalysisLauncher(view, vQuery, fileName));
        }
        if (ctx.column_names != null) {
            for (IdentifierContext column : ctx.column_names.identifier()) {
                view.addColumnName(column.getText());
            }
        }
        Storage_parametersContext storage = ctx.storage_parameters();
        if (storage != null) {
            List<Storage_parameter_optionContext> options = storage.storage_parameter_option();
            for (Storage_parameter_optionContext option : options) {
                String key = option.storage_parameter_name().getText();
                VexContext value = option.vex();
                fillOptionParams(value != null ? value.getText() : "", key, false, view::addOption);
            }
        }
        if (ctx.with_check_option() != null) {
            view.addOption(PgAbstractView.CHECK_OPTION,
                    ctx.with_check_option().LOCAL() != null ? "local" : "cascaded");
        }

        addSafe(getSchemaSafe(ids), view, ids);
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_CREATE, DbObjType.VIEW, getIdentifiers(context.name));
    }
}
