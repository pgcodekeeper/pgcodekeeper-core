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
package org.pgcodekeeper.core.parsers.antlr.ch.statement;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.base.QNameParser;
import org.pgcodekeeper.core.parsers.antlr.ch.launcher.ChFuncAnalysisLauncher;
import org.pgcodekeeper.core.parsers.antlr.ch.generated.CHParser.Create_function_stmtContext;
import org.pgcodekeeper.core.parsers.antlr.ch.generated.CHParser.Function_argumentsContext;
import org.pgcodekeeper.core.parsers.antlr.ch.generated.CHParser.Identifier_listContext;
import org.pgcodekeeper.core.database.base.schema.Argument;
import org.pgcodekeeper.core.database.ch.schema.ChDatabase;
import org.pgcodekeeper.core.database.ch.schema.ChFunction;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.List;

/**
 * Parser for ClickHouse CREATE FUNCTION statements.
 * Handles function creation including lambda expressions, function arguments, and body parsing.
 */
public final class CreateChFunction extends ChParserAbstract {

    private final Create_function_stmtContext ctx;

    /**
     * Creates a parser for ClickHouse CREATE FUNCTION statements.
     *
     * @param ctx      the ANTLR parse tree context for the CREATE FUNCTION statement
     * @param db       the ClickHouse database schema being processed
     * @param settings parsing configuration settings
     */
    public CreateChFunction(Create_function_stmtContext ctx, ChDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        List<ParserRuleContext> ids = getIdentifiers(ctx.qualified_name());

        String name = QNameParser.getFirstName(ids);
        ChFunction function = new ChFunction(name);
        parseObject(function);
        addSafe(db, function, ids);
    }

    /**
     * Parses function details including body expression and arguments.
     * Sets up expression analysis launcher for dependency tracking if enabled.
     *
     * @param function the function object to populate with parsed information
     */
    public void parseObject(ChFunction function) {
        var bodyCtx = ctx.lambda_expr().expr();
        function.setBody(getFullCtxText(bodyCtx));

        parseArgs(function, ctx.lambda_expr().function_arguments());

        db.addAnalysisLauncher(
                new ChFuncAnalysisLauncher(function, bodyCtx, fileName, settings.isEnableFunctionBodiesDependencies()));
    }

    private void parseArgs(ChFunction function, Function_argumentsContext funcArgs) {
        var id = funcArgs.identifier();
        if (id != null) {
            function.addArgument(new Argument(id.getText(), null));
            return;
        }

        Identifier_listContext argList = funcArgs.identifier_list();
        if (argList != null) {
            for (var argName : argList.identifier()) {
                function.addArgument(new Argument(argName.getText(), null));
            }
        }
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_CREATE, DbObjType.FUNCTION, ctx.qualified_name());
    }
}
