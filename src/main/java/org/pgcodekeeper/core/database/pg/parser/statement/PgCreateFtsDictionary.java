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
package org.pgcodekeeper.core.database.pg.parser.statement;

import java.util.List;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.base.parser.QNameParser;
import org.pgcodekeeper.core.database.pg.parser.generated.SQLParser.*;
import org.pgcodekeeper.core.database.pg.schema.*;
import org.pgcodekeeper.core.settings.ISettings;

/**
 * Parser for PostgreSQL CREATE TEXT SEARCH DICTIONARY statements.
 * <p>
 * This class handles parsing of full-text search dictionary definitions
 * including template references and dictionary-specific options. Text search
 * dictionaries process words during full-text search indexing and querying
 * operations.
 */
public final class PgCreateFtsDictionary extends PgParserAbstract {

    private final Create_fts_dictionary_statementContext ctx;

    /**
     * Constructs a new CreateFtsDictionary parser.
     *
     * @param ctx      the CREATE TEXT SEARCH DICTIONARY statement context
     * @param db       the PostgreSQL database object
     * @param settings the ISettings object
     */
    public PgCreateFtsDictionary(Create_fts_dictionary_statementContext ctx, PgDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        List<ParserRuleContext> ids = getIdentifiers(ctx.name);
        String name = QNameParser.getFirstName(ids);
        PgFtsDictionary dictionary = new PgFtsDictionary(name);
        for (Storage_parameter_optionContext option : ctx.storage_parameter_option()) {
            fillOptionParams(option.vex().getText(), option.storage_parameter_name().getText(), false, dictionary::addOption);
        }

        List<ParserRuleContext> templateIds = getIdentifiers(ctx.template);
        dictionary.setTemplate(getFullCtxText(ctx.template));
        addDepSafe(dictionary, templateIds, DbObjType.FTS_TEMPLATE);
        addSafe(getSchemaSafe(ids), dictionary, ids);
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_CREATE, DbObjType.FTS_DICTIONARY, getIdentifiers(ctx.name));
    }
}
