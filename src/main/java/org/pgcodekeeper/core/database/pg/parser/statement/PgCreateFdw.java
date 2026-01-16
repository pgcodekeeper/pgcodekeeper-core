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

import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.pg.parser.generated.SQLParser.*;
import org.pgcodekeeper.core.database.pg.schema.*;
import org.pgcodekeeper.core.settings.ISettings;

/**
 * Parser for PostgreSQL CREATE FOREIGN DATA WRAPPER statements.
 * <p>
 * This class handles parsing of foreign data wrapper definitions including
 * handler and validator functions, and wrapper-specific options. Foreign
 * data wrappers provide access to external data sources.
 */
public final class PgCreateFdw extends PgParserAbstract {

    /**
     * Function signature for foreign data wrapper validator functions.
     */
    public static final String VALIDATOR_SIGNATURE = "(text[], oid)";

    /**
     * Function signature for foreign data wrapper handler functions.
     */
    public static final String HANDLER_SIGNATURE = "()";

    private final Create_foreign_data_wrapper_statementContext ctx;

    /**
     * Constructs a new CreateFdw parser.
     *
     * @param ctx      the CREATE FOREIGN DATA WRAPPER statement context
     * @param db       the PostgreSQL database object
     * @param settings the ISettings object
     */
    public PgCreateFdw(Create_foreign_data_wrapper_statementContext ctx, PgDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        IdentifierContext nameCtx = ctx.name;
        PgForeignDataWrapper fDW = new PgForeignDataWrapper(nameCtx.getText());
        if (ctx.handler_func != null) {
            fDW.setHandler(getFullCtxText(ctx.handler_func));
            addDepSafe(fDW, getIdentifiers(ctx.handler_func), DbObjType.FUNCTION, HANDLER_SIGNATURE);
        }
        if (ctx.validator_func != null) {
            fDW.setValidator(getFullCtxText(ctx.validator_func));
            addDepSafe(fDW, getIdentifiers(ctx.validator_func), DbObjType.FUNCTION, VALIDATOR_SIGNATURE);
        }
        Define_foreign_optionsContext options = ctx.define_foreign_options();
        if (options != null) {
            for (Foreign_optionContext option : options.foreign_option()) {
                fillOptionParams(option.sconst().getText(), option.col_label().getText(), false, fDW::addOption);
            }
        }
        addSafe(db, fDW, List.of(nameCtx));
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_CREATE, DbObjType.FOREIGN_DATA_WRAPPER, ctx.name);
    }
}