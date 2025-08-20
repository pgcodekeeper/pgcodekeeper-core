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

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.base.QNameParser;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.Create_fts_parser_statementContext;
import org.pgcodekeeper.core.schema.pg.PgDatabase;
import org.pgcodekeeper.core.schema.pg.PgFtsParser;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.List;

/**
 * Parser for PostgreSQL CREATE TEXT SEARCH PARSER statements.
 * <p>
 * This class handles parsing of full-text search parser definitions
 * including start, gettoken, end, lextypes, and optional headline functions.
 * Text search parsers break documents into tokens for full-text search
 * indexing and provide token type information.
 */
public final class CreateFtsParser extends PgParserAbstract {

    private final Create_fts_parser_statementContext ctx;

    /**
     * Constructs a new CreateFtsParser parser.
     *
     * @param ctx      the CREATE TEXT SEARCH PARSER statement context
     * @param db       the PostgreSQL database object
     * @param settings the ISettings object
     */
    public CreateFtsParser(Create_fts_parser_statementContext ctx, PgDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        List<ParserRuleContext> ids = getIdentifiers(ctx.name);
        PgFtsParser parser = new PgFtsParser(QNameParser.getFirstName(ids));

        /*
         * function signatures are hardcoded for proper dependency resolution
         * argument list for each type of function is predetermined
         */

        parser.setStartFunction(getFullCtxText(ctx.start_func));
        addDepSafe(parser, getIdentifiers(ctx.start_func), DbObjType.FUNCTION, "(internal, integer)");

        parser.setGetTokenFunction(getFullCtxText(ctx.gettoken_func));
        addDepSafe(parser, getIdentifiers(ctx.gettoken_func), DbObjType.FUNCTION, "(internal, internal, internal)");

        parser.setEndFunction(getFullCtxText(ctx.end_func));
        addDepSafe(parser, getIdentifiers(ctx.end_func), DbObjType.FUNCTION, "(internal)");

        parser.setLexTypesFunction(getFullCtxText(ctx.lextypes_func));
        addDepSafe(parser, getIdentifiers(ctx.lextypes_func), DbObjType.FUNCTION, "(internal)");

        if (ctx.headline_func != null) {
            parser.setHeadLineFunction(getFullCtxText(ctx.headline_func));
            addDepSafe(parser, getIdentifiers(ctx.headline_func), DbObjType.FUNCTION, "(internal, internal, tsquery)");
        }

        addSafe(getSchemaSafe(ids), parser, ids);
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_CREATE, DbObjType.FTS_PARSER, getIdentifiers(ctx.name));
    }
}
