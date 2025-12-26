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
package org.pgcodekeeper.core.parsers.antlr.ch.statement;

import org.pgcodekeeper.core.parsers.antlr.ch.generated.CHParser.Create_user_stmtContext;
import org.pgcodekeeper.core.parsers.antlr.ch.generated.CHParser.HostContext;
import org.pgcodekeeper.core.database.ch.schema.ChDatabase;
import org.pgcodekeeper.core.database.ch.schema.ChUser;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.List;

/**
 * Parser for ClickHouse CREATE USER statements.
 * Handles user creation including host configurations, storage types, roles, grantees, and default database settings.
 */
public final class CreateChUser extends ChParserAbstract {

    private final Create_user_stmtContext ctx;

    /**
     * Creates a parser for ClickHouse CREATE USER statements.
     *
     * @param ctx      the ANTLR parse tree context for the CREATE USER statement
     * @param db       the ClickHouse database schema being processed
     * @param settings parsing configuration settings
     */
    public CreateChUser(Create_user_stmtContext ctx, ChDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        for (var userNameCtx : ctx.identifier_list().identifier()) {
            ChUser user = new ChUser(userNameCtx.getText());

            var host = ctx.host();
            if (host != null) {
                addHostType(host, user);
            }

            var storageType = ctx.storage;
            if (storageType != null) {
                user.setStorageType(storageType.getText());
            }
            addRoles(ctx.role, user, ChUser::addDefRole, ChUser::addExceptRole, "ALL");
            addRoles(ctx.grantees, user, ChUser::addGrantee, ChUser::addExGrantee, "ANY");

            var defDb = ctx.database;
            if (defDb != null) {
                user.setDefaultDatabase(defDb.getText());
            }

            addSafe(db, user, List.of(userNameCtx));
        }
    }

    private void addHostType(HostContext host, ChUser user) {
        if (host.ANY() != null) {
            return;
        }
        if (host.NONE() != null) {
            user.addHost("NONE");
            return;
        }
        var hostTypes = host.host_type();
        for (var hostType : hostTypes) {
            if (hostType.LOCAL() != null) {
                user.addHost("LOCAL");
            } else {
                var hostText = hostType.literal().getText();
                if (hostType.NAME() != null) {
                    user.addHost("NAME " + hostText);
                } else if (hostType.REGEXP() != null) {
                    user.addHost("REGEXP " + hostText);
                } else if (hostType.IP() != null) {
                    user.addHost("IP " + hostText);
                } else if (hostType.LIKE() != null) {
                    user.addHost("LIKE " + hostText);
                }
            }
        }
    }

    @Override
    protected String getStmtAction() {
        return "CREATE USER";
    }
}
