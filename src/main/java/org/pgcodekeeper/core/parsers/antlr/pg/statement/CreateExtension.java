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

import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.Create_extension_statementContext;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.IdentifierContext;
import org.pgcodekeeper.core.database.pg.schema.PgDatabase;
import org.pgcodekeeper.core.database.pg.schema.PgExtension;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.List;

/**
 * Parser for PostgreSQL CREATE EXTENSION statements.
 * <p>
 * This class handles parsing of extension creation statements, including
 * the extension name and optional schema specification where the extension
 * should be installed.
 */
public final class CreateExtension extends PgParserAbstract {

    private final Create_extension_statementContext ctx;

    /**
     * Constructs a new CreateExtension parser.
     *
     * @param ctx      the CREATE EXTENSION statement context
     * @param db       the PostgreSQL database object
     * @param settings the ISettings object
     */
    public CreateExtension(Create_extension_statementContext ctx, PgDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        IdentifierContext nameCtx = ctx.name;
        PgExtension ext = new PgExtension(nameCtx.getText());
        IdentifierContext id = ctx.schema;
        if (id != null) {
            ext.setSchema(id.getText());
            addDepSafe(ext, List.of(id), DbObjType.SCHEMA);
        }

        addSafe(db, ext, List.of(nameCtx));
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_CREATE, DbObjType.EXTENSION, ctx.name);
    }
}