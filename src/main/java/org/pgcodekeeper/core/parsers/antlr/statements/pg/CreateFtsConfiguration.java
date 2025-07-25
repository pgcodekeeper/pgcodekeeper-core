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

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.QNameParser;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.Create_fts_configuration_statementContext;
import org.pgcodekeeper.core.schema.pg.PgDatabase;
import org.pgcodekeeper.core.schema.pg.PgFtsConfiguration;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.List;

/**
 * Parser for PostgreSQL CREATE TEXT SEARCH CONFIGURATION statements.
 * <p>
 * This class handles parsing of full-text search configuration definitions
 * including parser references and configuration parameters. Text search
 * configurations define how documents are processed for full-text search
 * operations.
 */
public final class CreateFtsConfiguration extends PgParserAbstract {

    private final Create_fts_configuration_statementContext ctx;

    /**
     * Constructs a new CreateFtsConfiguration parser.
     *
     * @param ctx      the CREATE TEXT SEARCH CONFIGURATION statement context
     * @param db       the PostgreSQL database object
     * @param settings the ISettings object
     */
    public CreateFtsConfiguration(Create_fts_configuration_statementContext ctx, PgDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        List<ParserRuleContext> ids = getIdentifiers(ctx.name);
        String name = QNameParser.getFirstName(ids);
        PgFtsConfiguration config = new PgFtsConfiguration(name);
        if (ctx.parser_name != null) {
            List<ParserRuleContext> parserIds = getIdentifiers(ctx.parser_name);
            config.setParser(getFullCtxText(ctx.parser_name));
            addDepSafe(config, parserIds, DbObjType.FTS_PARSER);
        }
        addSafe(getSchemaSafe(ids), config, ids);
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_CREATE, DbObjType.FTS_CONFIGURATION,
                getIdentifiers(ctx.name));
    }
}
