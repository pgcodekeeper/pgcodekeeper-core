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

import java.util.Arrays;

import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.Create_foreign_data_wrapper_statementContext;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.Define_foreign_optionsContext;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.Foreign_optionContext;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.IdentifierContext;
import org.pgcodekeeper.core.schema.pg.PgDatabase;
import org.pgcodekeeper.core.schema.pg.PgForeignDataWrapper;
import org.pgcodekeeper.core.settings.ISettings;

public final class CreateFdw extends PgParserAbstract {

    public static final String VALIDATOR_SIGNATURE  = "(text[], oid)";
    public static final String HANDLER_SIGNATURE  = "()";

    private final Create_foreign_data_wrapper_statementContext ctx;

    public CreateFdw(Create_foreign_data_wrapper_statementContext ctx, PgDatabase db, ISettings settings) {
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
        if (options!= null) {
            for (Foreign_optionContext option : options.foreign_option()) {
                fillOptionParams(option.sconst().getText(), option.col_label().getText(), false, fDW::addOption);
            }
        }
        addSafe(db, fDW, Arrays.asList(nameCtx));
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_CREATE, DbObjType.FOREIGN_DATA_WRAPPER, ctx.name);
    }
}