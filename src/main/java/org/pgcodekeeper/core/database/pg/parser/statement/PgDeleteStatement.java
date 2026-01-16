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

import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.pg.parser.generated.SQLParser.Delete_stmt_for_psqlContext;
import org.pgcodekeeper.core.database.pg.schema.PgDatabase;
import org.pgcodekeeper.core.settings.ISettings;

/**
 * PostgreSQL DELETE statement parser that handles data deletion operations.
 * Extends {@link PgParserAbstract} to provide parsing functionality for
 * DELETE FROM statements which remove rows from a table.
 */
public final class PgDeleteStatement extends PgParserAbstract {

    private final Delete_stmt_for_psqlContext ctx;

    /**
     * Constructs a DELETE statement parser.
     *
     * @param ctx      the ANTLR parser context for the DELETE statement
     * @param db       the PostgreSQL database object
     * @param settings the ISettings object
     */
    public PgDeleteStatement(Delete_stmt_for_psqlContext ctx, PgDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        addObjReference(getIdentifiers(ctx.delete_table_name), DbObjType.TABLE, ACTION_DELETE);
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_DELETE + " FROM", DbObjType.TABLE, getIdentifiers(ctx.delete_table_name));
    }
}
