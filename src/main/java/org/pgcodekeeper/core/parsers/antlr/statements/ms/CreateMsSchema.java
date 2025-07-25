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
package org.pgcodekeeper.core.parsers.antlr.statements.ms;

import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.generated.TSQLParser.Create_schemaContext;
import org.pgcodekeeper.core.schema.AbstractSchema;
import org.pgcodekeeper.core.schema.ms.MsDatabase;
import org.pgcodekeeper.core.settings.ISettings;

/**
 * Parser for Microsoft SQL CREATE SCHEMA statements.
 * Handles schema creation including owner settings.
 */
public final class CreateMsSchema extends MsParserAbstract {

    private final Create_schemaContext ctx;

    /**
     * Creates a parser for Microsoft SQL CREATE SCHEMA statements.
     *
     * @param ctx      the ANTLR parse tree context for the CREATE SCHEMA statement
     * @param db       the Microsoft SQL database schema being processed
     * @param settings parsing configuration settings
     */
    public CreateMsSchema(Create_schemaContext ctx, MsDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        AbstractSchema schema = createAndAddSchemaWithCheck(ctx.schema_name);
        if (ctx.owner_name != null && !settings.isIgnorePrivileges()) {
            schema.setOwner(ctx.owner_name.getText());
        }
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_CREATE, DbObjType.SCHEMA, ctx.schema_name);
    }
}
