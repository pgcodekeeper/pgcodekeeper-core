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
package org.pgcodekeeper.core.parsers.antlr;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.loader.ParserListenerMode;
import org.pgcodekeeper.core.parsers.antlr.AntlrContextProcessor.ChSqlContextProcessor;
import org.pgcodekeeper.core.parsers.antlr.generated.CHParser.*;
import org.pgcodekeeper.core.parsers.antlr.statements.ch.*;
import org.pgcodekeeper.core.schema.ch.ChDatabase;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.utils.IMonitor;

import java.util.List;

/**
 * Custom ANTLR listener for parsing ClickHouse SQL statements.
 * Processes CREATE, ALTER, DROP statements and privilege grants,
 * building a database schema model from the parsed statements.
 */
public final class CustomChSQLParserListener extends CustomParserListener<ChDatabase> implements ChSqlContextProcessor {
    /**
     * Creates a new ClickHouse SQL parser listener.
     *
     * @param database the target database schema to populate
     * @param filename name of the file being parsed
     * @param mode     parsing mode
     * @param errors   list to collect parsing errors
     * @param monitor  progress monitor for cancellation support
     * @param settings application settings
     */
    public CustomChSQLParserListener(ChDatabase database, String filename, ParserListenerMode mode,
                                     List<Object> errors, IMonitor monitor, ISettings settings) {
        super(database, filename, mode, errors, monitor, settings);
    }

    @Override
    public void process(Ch_fileContext rootCtx, CommonTokenStream stream) {
        for (QueryContext query : rootCtx.query()) {
            query(query, stream);
        }
    }

    private void query(QueryContext query, CommonTokenStream stream) {
        var ddlStmt = query.stmt().ddl_stmt();
        if (ddlStmt != null) {
            Create_stmtContext createCtx = ddlStmt.create_stmt();
            Alter_stmtContext alter;
            Drop_stmtContext dropCtx;
            Privilegy_stmtContext privilStmt;
            if (createCtx != null) {
                create(createCtx, stream);
            } else if ((alter = ddlStmt.alter_stmt()) != null) {
                alter(alter);
            } else if ((dropCtx = ddlStmt.drop_stmt()) != null) {
                drop(dropCtx);
            } else if ((privilStmt = ddlStmt.privilegy_stmt()) != null) {
                safeParseStatement(new GrantChPrivilege(privilStmt, db, null, settings), ddlStmt);
            } else {
                addToQueries(query, getAction(query));
            }
        } else {
            addToQueries(query, getAction(query));
        }
    }

    private void create(Create_stmtContext ctx, CommonTokenStream stream) {
        ChParserAbstract p;
        Create_database_stmtContext createDatabase;
        Create_table_stmtContext createTable;
        Create_view_stmtContext createView;
        Create_function_stmtContext createFunc;
        Create_user_stmtContext createUser;
        Create_role_stmtContext createRole;
        Create_dictinary_stmtContext createDictionary;
        if ((createDatabase = ctx.create_database_stmt()) != null) {
            p = new CreateChSchema(createDatabase, db, settings);
        } else if ((createTable = ctx.create_table_stmt()) != null) {
            p = new CreateChTable(createTable, db, settings);
        } else if ((createView = ctx.create_view_stmt()) != null) {
            p = new CreateChView(createView, db, stream, settings);
        } else if ((createFunc = ctx.create_function_stmt()) != null) {
            p = new CreateChFunction(createFunc, db, settings);
        } else if ((createUser = ctx.create_user_stmt()) != null) {
            p = new CreateChUser(createUser, db, settings);
        } else if ((createRole = ctx.create_role_stmt()) != null) {
            p = new CreateChRole(createRole, db, settings);
        } else if (ctx.create_policy_stmt() != null) {
            p = new CreateChPolicy(ctx.create_policy_stmt(), db, settings);
        } else if ((createDictionary = ctx.create_dictinary_stmt()) != null) {
            p = new CreateChDictionary(createDictionary, db, settings);
        } else {
            addToQueries(ctx, getAction(ctx));
            return;
        }
        safeParseStatement(p, ctx);
    }

    private void drop(Drop_stmtContext ctx) {
        ChParserAbstract p;
        var element = ctx.drop_element();
        if (element.DATABASE() != null
                || element.FUNCTION() != null
                || element.TABLE() != null
                || element.VIEW() != null
                || element.USER() != null
                || element.ROLE() != null
                || element.DICTIONARY() != null) {
            p = new DropChStatement(ctx, db, settings);
        } else {
            addToQueries(ctx, getAction(ctx));
            return;
        }
        safeParseStatement(p, ctx);
    }

    private void alter(Alter_stmtContext ctx) {
        ChParserAbstract p;
        Alter_table_stmtContext altertableCtx = ctx.alter_table_stmt();
        if (altertableCtx != null) {
            p = new AlterChTable(altertableCtx, db, settings);
        } else if (ctx.alter_policy_stmt() != null
                || ctx.alter_user_stmt() != null
                || ctx.alter_role_stmt() != null) {
            p = new AlterChOther(ctx, db, settings);
        } else {
            addToQueries(ctx, getAction(ctx));
            return;
        }
        safeParseStatement(p, ctx);
    }

    private String getAction(ParserRuleContext ctx) {
        return getActionDescription(ctx, 1);
    }
}
