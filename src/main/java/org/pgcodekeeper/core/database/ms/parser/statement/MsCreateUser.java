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
package org.pgcodekeeper.core.database.ms.parser.statement;

import java.util.List;

import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.ms.MsDiffUtils;
import org.pgcodekeeper.core.database.ms.parser.generated.TSQLParser.*;
import org.pgcodekeeper.core.database.ms.schema.*;
import org.pgcodekeeper.core.settings.ISettings;

/**
 * Parser for Microsoft SQL CREATE USER statements.
 * Handles user creation including login association, default schema,
 * language settings, and encryption options.
 */
public final class MsCreateUser extends MsParserAbstract {

    private final Create_userContext ctx;

    /**
     * Creates a parser for Microsoft SQL CREATE USER statements.
     *
     * @param ctx      the ANTLR parse tree context for the CREATE USER statement
     * @param db       the Microsoft SQL database schema being processed
     * @param settings parsing configuration settings
     */
    public MsCreateUser(Create_userContext ctx, MsDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        IdContext nameCtx = ctx.user_name;
        MsUser user = new MsUser(nameCtx.getText());
        User_loginContext login = ctx.user_login();
        if (login != null && login.id() != null) {
            user.setLogin(login.id().getText());
        }

        for (User_optionContext option : ctx.user_option()) {
            if (option.schema_name != null) {
                user.setSchema(option.schema_name.getText());
            }
            if (option.DECIMAL() != null) {
                user.setLanguage(option.DECIMAL().getText());
            } else if (option.language_name_or_alias != null) {
                user.setLanguage(MsDiffUtils.quoteName(option.language_name_or_alias.getText()));
            }
            if (option.on_off() != null) {
                user.setAllowEncrypted(option.on_off().ON() != null);
            }
        }

        addSafe(db, user, List.of(nameCtx));
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_CREATE, DbObjType.USER, ctx.user_name);
    }
}
