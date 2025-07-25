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
package org.pgcodekeeper.core.parsers.antlr.statements.pg;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.QNameParser;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.*;
import org.pgcodekeeper.core.schema.AbstractSchema;
import org.pgcodekeeper.core.schema.AbstractTable;
import org.pgcodekeeper.core.schema.pg.GpExternalTable;
import org.pgcodekeeper.core.schema.pg.PgDatabase;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.List;
import java.util.Locale;

/**
 * Parser for Greenplum CREATE EXTERNAL TABLE statements.
 * <p>
 * This class handles parsing of Greenplum-specific external table definitions
 * including location specifications, execution commands, format options
 * (CSV, text, custom), encoding settings, error logging, and distribution
 * clauses. External tables provide access to data in external files or
 * through custom protocols.
 */
public final class CreateGpExternalTable extends TableAbstract {

    private final Create_table_external_statementContext ctx;

    /**
     * Constructs a new CreateGpExternalTable parser.
     *
     * @param db       the PostgreSQL database object
     * @param stream   the token stream for parsing
     * @param ctx      the CREATE EXTERNAL TABLE statement context
     * @param settings the ISettings object
     */
    public CreateGpExternalTable(PgDatabase db, CommonTokenStream stream,
                                 Create_table_external_statementContext ctx, ISettings settings) {
        super(db, stream, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        List<ParserRuleContext> ids = getIdentifiers(ctx.name);
        String tableName = QNameParser.getFirstName(ids);
        String schemaName = getSchemaNameSafe(ids);
        AbstractSchema schema = getSchemaSafe(ids);
        AbstractTable table = defineTable(tableName, schemaName);
        addSafe(schema, table, ids);
    }

    private AbstractTable defineTable(String tableName, String schemaName) {
        External_table_formatContext formatCtx = ctx.external_table_format();
        GpExternalTable table = new GpExternalTable(tableName);

        table.setWritable(ctx.WRITABLE() != null);
        table.setWeb(ctx.WEB() != null);
        fillColumns(ctx.define_table().define_columns(), table, schemaName, null);

        External_table_locationContext locationCtx = ctx.external_table_location();
        if (locationCtx != null) {
            for (var urLocation : locationCtx.sconst()) {
                table.addUrLocation(getFullCtxText(urLocation));
            }
            if (locationCtx.MASTER() != null) {
                table.setExLocation("ON MASTER");
            } else if (locationCtx.COORDINATOR() != null) {
                table.setExLocation("ON COORDINATOR");
            }
        }

        External_table_executeContext executeCtx = ctx.external_table_execute();
        if (executeCtx != null) {
            table.setCommand(getFullCtxText(executeCtx.command));
            if (executeCtx.ALL() != null) {
                table.setExLocation("ON ALL");
            } else if (executeCtx.MASTER() != null) {
                table.setExLocation("ON MASTER");
            } else if (executeCtx.COORDINATOR() != null) {
                table.setExLocation("ON COORDINATOR");
            } else if (executeCtx.segment_nubmer != null) {
                table.setExLocation("ON " + executeCtx.segment_nubmer.getText());
            } else if (executeCtx.HOST() != null) {
                if (executeCtx.hostname != null) {
                    table.setExLocation("ON HOST " + getFullCtxText(executeCtx.hostname));
                } else {
                    table.setExLocation("ON HOST");
                }
            } else if (executeCtx.SEGMENT() != null) {
                table.setExLocation("ON SEGMENT " + executeCtx.segment_id.getText());
            }
        }

        table.setFormatType(getFullCtxText(formatCtx.format_type));

        List<Format_optionsContext> formatOptions = formatCtx.format_options();
        if (!formatOptions.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (Format_optionsContext option : formatOptions) {
                if (option.HEADER() != null) {
                    sb.append("header ");
                } else if (option.FILL() != null) {
                    sb.append("fill missing fields ");
                } else if (option.FORCE() != null) {
                    sb.append("force not null ");
                    for (IdentifierContext identifier : option.identifier_list().identifier()) {
                        sb.append(identifier.getText()).append(",");
                    }
                    sb.setLength(sb.length() - 1);
                    sb.append(" ");
                } else if (option.FORMATTER() != null) {
                    sb.append("formatter=").append(getFullCtxText(option.sconst())).append(" ");
                } else {
                    sb.append(option.getChild(0).getText().toLowerCase(Locale.ROOT)).append(" ");
                    sb.append(getFullCtxText(option.sconst())).append(" ");
                }
            }
            sb.setLength(sb.length() - 1);
            table.setFormatOptions(sb.toString());
        }

        Define_foreign_optionsContext options = ctx.define_foreign_options();
        if (options != null) {
            for (Foreign_optionContext option : options.foreign_option()) {
                fillOptionParams(option.sconst().getText(), option.col_label().getText(), false,
                        table::addOption);
            }
        }

        if (ctx.ENCODING() != null) {
            table.setEncoding(getFullCtxText(ctx.sconst()));
        }

        External_table_logContext logCtx = ctx.external_table_log();
        if (logCtx != null) {
            table.setIsLogErrors(logCtx.LOG() != null);
            if (logCtx.PERSISTENTLY() != null) {
                table.addOption("error_log_persistent", "'true'");
            }

            table.setRejectLimit(Integer.parseInt(logCtx.iconst().getText()));
            table.setRowReject(logCtx.PERCENT() == null);
        }

        table.setDistribution(parseDistribution(ctx.distributed_clause()));
        return table;
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_CREATE, DbObjType.TABLE, getIdentifiers(ctx.name));
    }
}
