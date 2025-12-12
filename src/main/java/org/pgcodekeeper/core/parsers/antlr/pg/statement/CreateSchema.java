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

import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.Create_schema_statementContext;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.IdentifierContext;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.User_nameContext;
import org.pgcodekeeper.core.database.base.schema.AbstractSchema;
import org.pgcodekeeper.core.database.pg.schema.PgDatabase;
import org.pgcodekeeper.core.settings.ISettings;

/**
 * PostgreSQL CREATE SCHEMA statement parser that handles schema creation.
 * Extends {@link PgParserAbstract} to provide parsing functionality for
 * CREATE SCHEMA statements which create new database schemas with optional
 * ownership information.
 */
public final class CreateSchema extends PgParserAbstract {

    private final Create_schema_statementContext ctx;

    /**
     * Constructs a CREATE SCHEMA statement parser.
     *
     * @param ctx      the ANTLR parser context for the CREATE SCHEMA statement
     * @param db       the PostgreSQL database object
     * @param settings the ISettings object
     */
    public CreateSchema(Create_schema_statementContext ctx, PgDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        IdentifierContext nameCtx = ctx.name;
        if (nameCtx == null) {
            return;
        }

        AbstractSchema schema = createAndAddSchemaWithCheck(nameCtx);

        User_nameContext user = ctx.user_name();
        IdentifierContext userName = user == null ? null : user.identifier();
        if (userName != null && !settings.isIgnorePrivileges()
                && (!Consts.PUBLIC.equals(nameCtx.getText()) || !"postgres".equals(userName.getText()))) {
            schema.setOwner(userName.getText());
        }
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_CREATE, DbObjType.SCHEMA, ctx.name);
    }
}
