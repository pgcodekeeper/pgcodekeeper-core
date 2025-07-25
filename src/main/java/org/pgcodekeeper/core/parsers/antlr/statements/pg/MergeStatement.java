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

import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.Merge_stmt_for_psqlContext;
import org.pgcodekeeper.core.schema.pg.PgDatabase;
import org.pgcodekeeper.core.settings.ISettings;

/**
 * PostgreSQL MERGE statement parser that handles table merge operations.
 * Extends {@link PgParserAbstract} to provide parsing functionality for
 * MERGE INTO statements which conditionally insert, update, or delete rows
 * in a target table based on matching criteria.
 */
public final class MergeStatement extends PgParserAbstract {

    private final Merge_stmt_for_psqlContext ctx;

    /**
     * Constructs a MERGE statement parser.
     *
     * @param ctx      the ANTLR parser context for the MERGE statement
     * @param db       the PostgreSQL database object
     * @param settings the ISettings object
     */
    public MergeStatement(Merge_stmt_for_psqlContext ctx, PgDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        addObjReference(getIdentifiers(ctx.merge_table_name), DbObjType.TABLE, ACTION_MERGE);
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_MERGE + " INTO", DbObjType.TABLE, getIdentifiers(ctx.merge_table_name));
    }
}
