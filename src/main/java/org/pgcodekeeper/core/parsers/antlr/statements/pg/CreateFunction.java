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
package org.pgcodekeeper.core.parsers.antlr.statements.pg;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.Consts.FUNC_SIGN;
import org.pgcodekeeper.core.PgDiffUtils;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.*;
import org.pgcodekeeper.core.parsers.antlr.expr.launcher.FuncProcAnalysisLauncher;
import org.pgcodekeeper.core.parsers.antlr.expr.launcher.VexAnalysisLauncher;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.*;
import org.pgcodekeeper.core.schema.Argument;
import org.pgcodekeeper.core.schema.GenericColumn;
import org.pgcodekeeper.core.schema.pg.AbstractPgFunction;
import org.pgcodekeeper.core.schema.pg.PgDatabase;
import org.pgcodekeeper.core.schema.pg.PgFunction;
import org.pgcodekeeper.core.schema.pg.PgProcedure;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.utils.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

/**
 * Parser for PostgreSQL CREATE FUNCTION and CREATE PROCEDURE statements.
 * <p>
 * This class handles parsing of function and procedure definitions, including
 * their parameters, return types, body, language, and various attributes such as
 * volatility, security, parallel execution, and configuration parameters.
 */
public final class CreateFunction extends PgParserAbstract {

    /**
     * Default value constant used in configuration parameters.
     */
    public static final String DEFAULT = "DEFAULT";

    private final List<Object> errors;
    private final Queue<AntlrTask<?>> antlrTasks;
    private final Create_function_statementContext ctx;

    /**
     * Constructs a new CreateFunction parser.
     *
     * @param ctx        the CREATE FUNCTION statement context
     * @param db         the PostgreSQL database object
     * @param errors     list to collect parsing errors
     * @param antlrTasks queue for scheduling ANTLR analysis tasks
     * @param settings   the ISettings object
     */
    public CreateFunction(Create_function_statementContext ctx, PgDatabase db,
                          List<Object> errors, Queue<AntlrTask<?>> antlrTasks, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
        this.errors = errors;
        this.antlrTasks = antlrTasks;
    }

    @Override
    public void parseObject() {
        List<ParserRuleContext> ids = getIdentifiers(ctx.function_parameters().schema_qualified_name());
        String name = QNameParser.getFirstName(ids);
        AbstractPgFunction function = ctx.PROCEDURE() != null ? new PgProcedure(name)
                : new PgFunction(name);

        fillFunction(function);

        if (ctx.ret_table != null) {
            StringBuilder sb = new StringBuilder();
            for (Function_column_name_typeContext ret_col : ctx.ret_table.function_column_name_type()) {
                String columnName = ret_col.identifier().getText();
                String type = getTypeName(ret_col.data_type());
                addTypeDepcy(ret_col.data_type(), function);
                function.addReturnsColumn(ret_col.identifier().getText(), type);
                sb.append(PgDiffUtils.getQuotedName(columnName)).append(" ").append(type).append(", ");
            }
            sb.setLength(sb.length() - 2);
            function.setReturns("TABLE(" + sb + ")");
        } else if (ctx.rettype_data != null) {
            function.setReturns(getTypeName(ctx.rettype_data));
            addTypeDepcy(ctx.rettype_data, function);
        }
        addSafe(getSchemaSafe(ids), function, ids, parseArguments(ctx.function_parameters().function_args()));
    }

    private void fillFunction(AbstractPgFunction function) {
        Function_defContext funcDef = null;
        Float cost = null;
        String language = null;
        for (Function_actions_commonContext action : ctx.function_actions_common()) {
            if (action.WINDOW() != null) {
                function.setWindow(true);
            } else if (action.IMMUTABLE() != null) {
                function.setVolatileType("IMMUTABLE");
            } else if (action.STABLE() != null) {
                function.setVolatileType("STABLE");
            } else if (action.STRICT() != null || action.RETURNS() != null) {
                function.setStrict(true);
            } else if (action.DEFINER() != null) {
                function.setSecurityDefiner(true);
            } else if (action.LEAKPROOF() != null) {
                function.setLeakproof(action.NOT() == null);
            } else if (action.COORDINATOR() != null) {
                function.setExecuteOn("COORDINATOR");
            } else if (action.SEGMENTS() != null) {
                function.setExecuteOn("ALL SEGMENTS");
            } else if (action.MASTER() != null) {
                function.setExecuteOn("MASTER");
            } else if (action.INITPLAN() != null) {
                function.setExecuteOn("INITPLAN");
            } else if (action.LANGUAGE() != null) {
                language = action.lang_name.getText();
            } else if (action.COST() != null) {
                cost = Float.parseFloat(action.execution_cost.getText());
            } else if (action.ROWS() != null) {
                float f = Float.parseFloat(action.result_rows.getText());
                if (0.0f != f) {
                    function.setRows(Float.parseFloat(action.result_rows.getText()));
                }
            } else if (action.AS() != null) {
                funcDef = action.function_def();
                StringBuilder sb = new StringBuilder();
                if (funcDef.symbol != null) {
                    String probin = unquoteQuotedString(funcDef.definition).getFirst();
                    String symbol = unquoteQuotedString(funcDef.symbol).getFirst();

                    sb.append(PgDiffUtils.quoteString(probin)).append(", ");

                    if (!symbol.contains("'") && !symbol.contains("\\")) {
                        sb.append(PgDiffUtils.quoteString(symbol));
                    } else {
                        sb.append(PgDiffUtils.quoteStringDollar(symbol));
                    }
                } else {
                    String definition = unquoteQuotedString(funcDef.definition).getFirst();
                    sb.append(PgDiffUtils.quoteStringDollar(definition));
                }

                function.setBody(checkNewLines(sb.toString()));
            } else if (action.TRANSFORM() != null) {
                for (Transform_for_typeContext transform : action.transform_for_type()) {
                    function.addTransform(getFullCtxText(transform.data_type()));
                }
            } else if (action.SAFE() != null) {
                function.setParallel("SAFE");
            } else if (action.RESTRICTED() != null) {
                function.setParallel("RESTRICTED");
            } else if (action.SET() != null) {
                setConfigParams(action, function);
            } else if (action.SUPPORT() != null) {
                List<ParserRuleContext> suppFuncIds = getIdentifiers(action.schema_qualified_name());
                function.setSupportFunc(getFullCtxText(suppFuncIds));
                addDepSafe(function, suppFuncIds, DbObjType.FUNCTION, FUNC_SIGN.INTERNAL.getName());
            }
        }

        List<Pair<String, GenericColumn>> funcArgs = fillArguments(function);

        Function_bodyContext body = ctx.function_body();
        if (body != null) {
            function.setInStatementBody(true);
            function.setBody(getFullCtxTextWithCheckNewLines(body));
            if (language == null) {
                language = "sql";
            }
            analyzeFunctionBody(function, body, funcArgs);
        } else if (funcDef != null && funcDef.symbol == null && PgDiffUtils.isValidLanguage(language)) {
            analyzeFunctionDefinition(function, language, funcDef.definition, funcArgs);
        }

        With_storage_parameterContext storage = ctx.with_storage_parameter();
        if (storage != null) {
            for (Storage_parameter_optionContext option : storage.storage_parameters().storage_parameter_option()) {
                if ("isStrict".equalsIgnoreCase(option.getText())) {
                    function.setStrict(true);
                } else if ("isCachable".equalsIgnoreCase(option.getText())) {
                    function.setVolatileType("IMMUTABLE");
                }
            }
        }

        function.setLanguageCost(language, cost);
    }

    private void setConfigParams(Function_actions_commonContext action, AbstractPgFunction function) {
        Session_local_optionContext setOpts = action.session_local_option();
        if (setOpts.XML() != null) {
            function.addConfiguration("xmloption", setOpts.DOCUMENT() != null ? "'document'" : "'content'");
            return;
        }

        if (setOpts.ROLE() != null) {
            String role = setOpts.role_param.getText();
            String val = role.equalsIgnoreCase("none") ? "none" : role;
            function.addConfiguration("role", PgDiffUtils.quoteString(val));
            return;
        }

        if (setOpts.SESSION() != null) {
            String val;
            if (setOpts.sconst() != null) {
                val = setOpts.sconst().getText();
            } else if (setOpts.session_param != null) {
                val = PgDiffUtils.quoteString(setOpts.session_param.getText());
            } else {
                return;
            }
            function.addConfiguration("session_authorization", val);
            return;
        }

        Zone_valueContext timezone = setOpts.zone_value();
        if (timezone != null) {
            if (timezone.DEFAULT() != null) {
                return;
            }

            String value = getFullCtxText(timezone);
            if (!"LOCAL".equalsIgnoreCase(value)) {
                function.addConfiguration("\"TimeZone\"", value);
            }
            return;
        }

        IdentifierContext scope = setOpts.config_scope;
        String configParam = setOpts.config_param.getText();
        String par;
        if (scope != null) {
            par = PgDiffUtils.getQuotedName(scope.getText() + '.' + configParam);
        } else {
            par = PgDiffUtils.getQuotedName(configParam);
            par = par.equalsIgnoreCase("timezone") ? "\"TimeZone\"" : par;
        }

        if (setOpts.FROM() != null) {
            function.addConfiguration(par, AbstractPgFunction.FROM_CURRENT);
            return;
        }

        Set_statement_valueContext set = setOpts.set_statement_value();
        if (set.DEFAULT() != null) {
            function.addConfiguration(par, DEFAULT);
            return;
        }

        StringBuilder sb = new StringBuilder();
        for (VexContext val : set.vex()) {
            sb.append(getFullCtxText(val)).append(", ");
        }
        sb.setLength(sb.length() - 2);
        function.addConfiguration(par, sb.toString());
    }

    private void analyzeFunctionDefinition(AbstractPgFunction function, String language,
                                           SconstContext definition, List<Pair<String, GenericColumn>> funcArgs) {
        Pair<String, Token> pair = unquoteQuotedString(definition);
        String def = pair.getFirst();
        Token start = pair.getSecond();

        // Parsing the function definition and adding its result context for analysis.
        // Adding contexts of function arguments for analysis.

        String name = "function definition of " + function.getBareName();
        List<Object> err = new ArrayList<>();

        if ("SQL".equalsIgnoreCase(language)) {
            AntlrTaskManager.submit(antlrTasks,
                    () -> AntlrParser.createSQLParser(def, name, err, start).sql(),
                    funcCtx -> {
                        errors.addAll(err);
                        FuncProcAnalysisLauncher launcher = new FuncProcAnalysisLauncher(
                                function, funcCtx, fileName, funcArgs, settings.isEnableFunctionBodiesDependencies());
                        launcher.setOffset(start);
                        db.addAnalysisLauncher(launcher);
                    });
        } else if ("PLPGSQL".equalsIgnoreCase(language)) {
            AntlrTaskManager.submit(antlrTasks,
                    () -> {
                        var parser = AntlrParser.createSQLParser(def, name, err, start);
                        AntlrUtils.removeIntoStatements(parser);
                        return parser.plpgsql_function();
                    },
                    funcCtx -> {
                        errors.addAll(err);
                        FuncProcAnalysisLauncher launcher = new FuncProcAnalysisLauncher(
                                function, funcCtx, fileName, funcArgs, settings.isEnableFunctionBodiesDependencies());
                        launcher.setOffset(start);
                        db.addAnalysisLauncher(launcher);
                    });
        }
    }

    private void analyzeFunctionBody(AbstractPgFunction function, Function_bodyContext body,
                                     List<Pair<String, GenericColumn>> funcArgs) {
        // finalizer-only task, defers analyzer until finalizing stage
        AntlrTaskManager.submit(antlrTasks, () -> body,
                funcCtx -> {
                    FuncProcAnalysisLauncher launcher = new FuncProcAnalysisLauncher(
                            function, funcCtx, fileName, funcArgs, settings.isEnableFunctionBodiesDependencies());
                    db.addAnalysisLauncher(launcher);
                });
    }

    /**
     * Returns a list of pairs, each of which contains the name of the argument
     * and its full type name in GenericColumn object (typeSchema, typeName, DbObjType.TYPE).
     */
    private List<Pair<String, GenericColumn>> fillArguments(AbstractPgFunction function) {
        List<Pair<String, GenericColumn>> funcArgs = new ArrayList<>();
        for (Function_argumentsContext argument : ctx.function_parameters()
                .function_args().function_arguments()) {
            Identifier_nontypeContext name = argument.identifier_nontype();
            String argName = name != null ? name.getText() : null;
            String typeSchema = Consts.PG_CATALOG;
            String typeName;

            Data_typeContext dataType = argument.data_type();
            Schema_qualified_name_nontypeContext typeQname = dataType.predefined_type()
                    .schema_qualified_name_nontype();
            if (typeQname != null) {
                List<ParserRuleContext> typeIds = getIdentifiers(typeQname);
                typeSchema = QNameParser.getSchemaName(typeIds);
                typeName = QNameParser.getFirstName(typeIds);
            } else {
                typeName = getFullCtxText(dataType);
            }

            Argument arg = new Argument(parseArgMode(argument.argmode()),
                    argName, getTypeName(dataType));
            addTypeDepcy(dataType, function);

            VexContext def = argument.vex();
            if (def != null) {
                arg.setDefaultExpression(getFullCtxTextWithCheckNewLines(def));
                db.addAnalysisLauncher(new VexAnalysisLauncher(function, def, fileName));
            }

            function.addArgument(arg);
            funcArgs.add(new Pair<>(argName, new GenericColumn(typeSchema, typeName, DbObjType.TYPE)));
        }
        return funcArgs;
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_CREATE,
                ctx.PROCEDURE() != null ? DbObjType.PROCEDURE : DbObjType.FUNCTION,
                getIdentifiers(ctx.function_parameters().schema_qualified_name()));
    }
}
