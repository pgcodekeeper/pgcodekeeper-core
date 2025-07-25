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
import org.pgcodekeeper.core.DangerStatement;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.Update_stmt_for_psqlContext;
import org.pgcodekeeper.core.schema.PgObjLocation;
import org.pgcodekeeper.core.schema.pg.PgDatabase;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.List;

/**
 * PostgreSQL UPDATE statement parser that handles data modification operations.
 * Extends {@link PgParserAbstract} to provide parsing functionality for
 * UPDATE statements which modify existing rows in a table. This parser
 * automatically marks the operation as dangerous due to data modification risks.
 */
public final class UpdateStatement extends PgParserAbstract {

    private final Update_stmt_for_psqlContext ctx;

    /**
     * Constructs an UPDATE statement parser.
     *
     * @param ctx      the ANTLR parser context for the UPDATE statement
     * @param db       the PostgreSQL database object
     * @param settings the ISettings object
     */
    public UpdateStatement(Update_stmt_for_psqlContext ctx, PgDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        List<ParserRuleContext> ids = getIdentifiers(ctx.update_table_name);
        PgObjLocation loc = addObjReference(ids, DbObjType.TABLE, ACTION_UPDATE);
        loc.setWarning(DangerStatement.UPDATE);
    }

    @Override
    protected PgObjLocation fillQueryLocation(ParserRuleContext ctx) {
        PgObjLocation loc = super.fillQueryLocation(ctx);
        loc.setWarning(DangerStatement.UPDATE);
        return loc;
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_UPDATE, DbObjType.TABLE, getIdentifiers(ctx.update_table_name));
    }
}
