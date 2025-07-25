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
import org.pgcodekeeper.core.parsers.antlr.generated.TSQLParser.Alter_db_roleContext;
import org.pgcodekeeper.core.schema.ms.MsDatabase;
import org.pgcodekeeper.core.schema.ms.MsRole;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.Collections;

/**
 * Parser for Microsoft SQL ALTER ROLE statements.
 * Handles role modifications including adding members to database roles.
 */
public class AlterMsRole extends MsParserAbstract {

    private final Alter_db_roleContext ctx;

    /**
     * Creates a parser for Microsoft SQL ALTER ROLE statements.
     *
     * @param ctx      the ANTLR parse tree context for the ALTER ROLE statement
     * @param db       the Microsoft SQL database schema being processed
     * @param settings parsing configuration settings
     */
    public AlterMsRole(Alter_db_roleContext ctx, MsDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        MsRole role = getSafe(MsDatabase::getRole, db, ctx.role_name);

        if (ctx.ADD() != null) {
            doSafe(MsRole::addMember, role, ctx.database_principal.getText());
        }

        addObjReference(Collections.singletonList(ctx.role_name), DbObjType.ROLE, ACTION_ALTER);
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_ALTER, DbObjType.ROLE, ctx.role_name);
    }
}
