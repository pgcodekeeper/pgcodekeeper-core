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
import org.pgcodekeeper.core.database.pg.parser.generated.SQLParser.Insert_stmt_for_psqlContext;
import org.pgcodekeeper.core.database.pg.schema.PgDatabase;
import org.pgcodekeeper.core.settings.ISettings;

/**
 * PostgreSQL INSERT statement parser that handles data insertion operations.
 * Extends {@link PgParserAbstract} to provide parsing functionality for
 * INSERT INTO statements which add new rows to a table.
 */
public final class PgInsertStatement extends PgParserAbstract {

    private final Insert_stmt_for_psqlContext ctx;

    /**
     * Constructs an INSERT statement parser.
     *
     * @param ctx      the ANTLR parser context for the INSERT statement
     * @param db       the PostgreSQL database object
     * @param settings the ISettings object
     */
    public PgInsertStatement(Insert_stmt_for_psqlContext ctx, PgDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        addObjReference(getIdentifiers(ctx.insert_table_name), DbObjType.TABLE, ACTION_INSERT);
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_INSERT + " INTO", DbObjType.TABLE, getIdentifiers(ctx.insert_table_name));
    }
}
