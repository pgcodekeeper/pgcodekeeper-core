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
package org.pgcodekeeper.core.parsers.antlr.pg.statement;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.base.QNameParser;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.Alter_fts_configurationContext;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.Alter_fts_statementContext;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.IdentifierContext;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.Schema_qualified_nameContext;
import org.pgcodekeeper.core.database.pg.schema.PgDatabase;
import org.pgcodekeeper.core.database.pg.schema.PgFtsConfiguration;
import org.pgcodekeeper.core.database.pg.schema.PgSchema;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.ArrayList;
import java.util.List;

/**
 * Parser for PostgreSQL ALTER TEXT SEARCH statements.
 * <p>
 * This class handles parsing of various ALTER statements for full-text search
 * objects including configurations, dictionaries, templates, and parsers.
 * Supports modifying configuration mappings and other properties of
 * text search objects.
 */
public final class AlterFtsStatement extends PgParserAbstract {

    private final Alter_fts_statementContext ctx;

    /**
     * Constructs a new AlterFtsStatement parser.
     *
     * @param ctx      the ALTER TEXT SEARCH statement context
     * @param db       the PostgreSQL database object
     * @param settings the ISettings object
     */
    public AlterFtsStatement(Alter_fts_statementContext ctx, PgDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        List<ParserRuleContext> ids = getIdentifiers(ctx.name);

        DbObjType tt;
        if (ctx.DICTIONARY() != null) {
            tt = DbObjType.FTS_DICTIONARY;
        } else if (ctx.TEMPLATE() != null) {
            tt = DbObjType.FTS_TEMPLATE;
        } else if (ctx.PARSER() != null) {
            tt = DbObjType.FTS_PARSER;
        } else {
            tt = DbObjType.FTS_CONFIGURATION;
        }

        addObjReference(ids, tt, ACTION_ALTER);

        if (tt != DbObjType.FTS_CONFIGURATION) {
            return;
        }

        PgFtsConfiguration config = getSafe(PgSchema::getFtsConfiguration,
                getSchemaSafe(ids), QNameParser.getFirstNameCtx(ids));

        Alter_fts_configurationContext afc = ctx.alter_fts_configuration();
        if (afc != null && afc.ADD() != null) {
            for (IdentifierContext type : afc.identifier_list().identifier()) {
                List<String> dics = new ArrayList<>();
                for (Schema_qualified_nameContext dictionary : afc.schema_qualified_name()) {
                    List<ParserRuleContext> dIds = getIdentifiers(dictionary);
                    dics.add(getFullCtxText(dictionary));
                    addDepSafe(config, dIds, DbObjType.FTS_DICTIONARY);
                }

                doSafe((s, o) -> s.addDictionary(getFullCtxText(type), dics),
                        config, null);
            }
        }
    }

    @Override
    protected String getStmtAction() {
        DbObjType ftsType;
        if (ctx.DICTIONARY() != null) {
            ftsType = DbObjType.FTS_DICTIONARY;
        } else if (ctx.TEMPLATE() != null) {
            ftsType = DbObjType.FTS_TEMPLATE;
        } else if (ctx.PARSER() != null) {
            ftsType = DbObjType.FTS_PARSER;
        } else {
            ftsType = DbObjType.FTS_CONFIGURATION;
        }
        return getStrForStmtAction(ACTION_ALTER, ftsType, getIdentifiers(ctx.name));
    }
}
