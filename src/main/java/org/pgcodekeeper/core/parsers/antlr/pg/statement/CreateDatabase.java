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

import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.Create_database_statementContext;
import org.pgcodekeeper.core.schema.pg.PgDatabase;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.Collections;

/**
 * PostgreSQL CREATE DATABASE statement parser that handles database creation.
 * Extends {@link PgParserAbstract} to provide parsing functionality for
 * CREATE DATABASE statements which create new PostgreSQL databases.
 */
public final class CreateDatabase extends PgParserAbstract {

    private final Create_database_statementContext ctx;

    /**
     * Constructs a new CreateDatabase parser.
     *
     * @param ctx      the CREATE DATABASE statement context
     * @param db       the PostgreSQL database object
     * @param settings the ISettings object
     */
    public CreateDatabase(Create_database_statementContext ctx, PgDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        addObjReference(Collections.singletonList(ctx.identifier()), DbObjType.DATABASE, ACTION_CREATE);
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_CREATE, DbObjType.DATABASE, Collections.singletonList(ctx.identifier()));
    }
}