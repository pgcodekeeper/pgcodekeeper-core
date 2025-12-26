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

import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.ch.generated.CHParser.Alter_policy_stmtContext;
import org.pgcodekeeper.core.parsers.antlr.ch.generated.CHParser.Alter_role_stmtContext;
import org.pgcodekeeper.core.parsers.antlr.ch.generated.CHParser.Alter_stmtContext;
import org.pgcodekeeper.core.parsers.antlr.ch.generated.CHParser.Alter_user_stmtContext;
import org.pgcodekeeper.core.database.ch.schema.ChDatabase;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.Arrays;
import java.util.Collections;

/**
 * Parser for ClickHouse ALTER statements that handle policies, users, and roles.
 * Processes ALTER POLICY, ALTER USER, and ALTER ROLE statements.
 */
public final class AlterChOther extends ChParserAbstract {

    private final Alter_stmtContext ctx;

    /**
     * Creates a parser for ClickHouse ALTER statements.
     *
     * @param ctx      the ANTLR parse tree context for the ALTER statement
     * @param db       the ClickHouse database schema being processed
     * @param settings parsing configuration settings
     */
    public AlterChOther(Alter_stmtContext ctx, ChDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    /**
     * Parses the ALTER statement and processes the appropriate database object type.
     * Handles ALTER POLICY, ALTER USER, or ALTER ROLE statements based on the context.
     */
    @Override
    public void parseObject() {
        var alterPolicyCtx = ctx.alter_policy_stmt();
        var alterUserCtx = ctx.alter_user_stmt();
        var alterRoleCtx = ctx.alter_role_stmt();
        if (alterPolicyCtx != null) {
            alterPolicy(alterPolicyCtx);
        } else if (alterUserCtx != null) {
            alterUser(alterUserCtx);
        } else if (alterRoleCtx != null) {
            alterRole(alterRoleCtx);
        }
    }

    private void alterPolicy(Alter_policy_stmtContext ctx) {
        for (var polName : ctx.policy_name()) {
            for (var tableNameCtx : polName.qualified_name_or_asterisk()) {
                for (var policyNameCtx : polName.identifier()) {
                    addObjReference(Arrays.asList(tableNameCtx, policyNameCtx), DbObjType.POLICY, ACTION_ALTER);
                }
            }
        }
    }

    private void alterUser(Alter_user_stmtContext ctx) {
        for (var userNameCtx : ctx.identifier()) {
            addObjReference(Collections.singletonList(userNameCtx), DbObjType.USER, ACTION_ALTER);
        }
    }

    private void alterRole(Alter_role_stmtContext ctx) {
        for (var roleCtx : ctx.identifier()) {
            addObjReference(Collections.singletonList(roleCtx), DbObjType.ROLE, ACTION_ALTER);
        }
    }

    @Override
    protected String getStmtAction() {
        if (ctx.alter_policy_stmt() != null) {
            return "ALTER POLICY";
        }
        if (ctx.alter_user_stmt() != null) {
            return "ALTER USER";
        }
        if (ctx.alter_role_stmt() != null) {
            return "ALTER ROLE";
        }
        return null;
    }
}
