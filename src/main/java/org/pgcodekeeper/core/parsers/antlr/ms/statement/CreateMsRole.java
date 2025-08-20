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
package org.pgcodekeeper.core.parsers.antlr.ms.statement;

import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.Create_db_roleContext;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.IdContext;
import org.pgcodekeeper.core.schema.ms.MsDatabase;
import org.pgcodekeeper.core.schema.ms.MsRole;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.List;

/**
 * Parser for Microsoft SQL CREATE ROLE statements.
 * Handles database role creation including owner settings.
 */
public final class CreateMsRole extends MsParserAbstract {

    private final Create_db_roleContext ctx;

    /**
     * Creates a parser for Microsoft SQL CREATE ROLE statements.
     *
     * @param ctx      the ANTLR parse tree context for the CREATE ROLE statement
     * @param db       the Microsoft SQL database schema being processed
     * @param settings parsing configuration settings
     */
    public CreateMsRole(Create_db_roleContext ctx, MsDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        IdContext nameCtx = ctx.role_name;
        String name = nameCtx.getText();
        MsRole role = new MsRole(name);
        if (ctx.owner_name != null && !settings.isIgnorePrivileges()) {
            role.setOwner(ctx.owner_name.getText());
        }

        addSafe(db, role, List.of(nameCtx));
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_CREATE, DbObjType.ROLE, ctx.role_name);
    }
}
