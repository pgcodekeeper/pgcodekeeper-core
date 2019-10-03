package cz.startnet.utils.pgdiff.parsers.antlr.statements;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.tree.TerminalNode;

import cz.startnet.utils.pgdiff.PgDiffUtils;
import cz.startnet.utils.pgdiff.parsers.antlr.AntlrError;
import cz.startnet.utils.pgdiff.parsers.antlr.AntlrParser;
import cz.startnet.utils.pgdiff.parsers.antlr.AntlrTask;
import cz.startnet.utils.pgdiff.parsers.antlr.QNameParser;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Character_stringContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Create_funct_paramsContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Create_function_statementContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Function_actions_commonContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Function_argumentsContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Function_column_name_typeContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Function_defContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.IdentifierContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Schema_qualified_name_nontypeContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Set_statement_valueContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Storage_parameter_optionContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Transform_for_typeContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.With_storage_parameterContext;
import cz.startnet.utils.pgdiff.parsers.antlr.expr.launcher.FuncProcAnalysisLauncher;
import cz.startnet.utils.pgdiff.schema.AbstractPgFunction;
import cz.startnet.utils.pgdiff.schema.Argument;
import cz.startnet.utils.pgdiff.schema.GenericColumn;
import cz.startnet.utils.pgdiff.schema.PgDatabase;
import cz.startnet.utils.pgdiff.schema.PgFunction;
import cz.startnet.utils.pgdiff.schema.PgProcedure;
import cz.startnet.utils.pgdiff.schema.StatementActions;
import ru.taximaxim.codekeeper.apgdiff.ApgdiffConsts;
import ru.taximaxim.codekeeper.apgdiff.model.difftree.DbObjType;
import ru.taximaxim.codekeeper.apgdiff.utils.Pair;

public class CreateFunction extends ParserAbstract {

    private final List<AntlrError> errors;
    private final Queue<AntlrTask<?>> antlrTasks;
    private final Create_function_statementContext ctx;


    public CreateFunction(Create_function_statementContext ctx, PgDatabase db,
            List<AntlrError> errors, Queue<AntlrTask<?>> antlrTasks) {
        super(db);
        this.ctx = ctx;
        this.errors = errors;
        this.antlrTasks = antlrTasks;
    }

    @Override
    public void parseObject() {
        List<IdentifierContext> ids = ctx.function_parameters().name.identifier();
        String name = QNameParser.getFirstName(ids);
        AbstractPgFunction function = ctx.PROCEDURE() != null ? new PgProcedure(name)
                : new PgFunction(name);

        fillFunction(ctx.funct_body, function, fillArguments(function));

        if (ctx.ret_table != null) {
            function.setReturns(getFullCtxText(ctx.ret_table));
            for (Function_column_name_typeContext ret_col : ctx.ret_table.function_column_name_type()) {
                addPgTypeDepcy(ret_col.column_type, function);
                function.addReturnsColumn(ret_col.column_name.getText(), getTypeName(ret_col.column_type));
            }
        } else if (ctx.rettype_data != null) {
            function.setReturns(getTypeName(ctx.rettype_data));
            addPgTypeDepcy(ctx.rettype_data, function);
        }
        addSafe(getSchemaSafe(ids), function, ids);
    }

    private void fillFunction(Create_funct_paramsContext params,
            AbstractPgFunction function, List<Pair<String, GenericColumn>> funcArgs) {
        Function_defContext funcDef = null;
        Float cost = null;
        String language = null;
        for (Function_actions_commonContext action  : params.function_actions_common()) {
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
                function.setLeakproof(true);
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
                function.setBody(db.getArguments(), getFullCtxText(funcDef));
            } else if (action.TRANSFORM() != null) {
                for (Transform_for_typeContext transform : action.transform_for_type()) {
                    function.addTransform(ParserAbstract.getFullCtxText(transform.type_name));
                }
            } else if (action.SAFE() != null) {
                function.setParallel("SAFE");
            } else if (action.RESTRICTED() != null) {
                function.setParallel("RESTRICTED");
            } else if (action.SET() != null) {
                String par = PgDiffUtils.getQuotedName(action.configuration_parameter.getText());
                if (action.FROM() != null) {
                    function.addConfiguration(par, AbstractPgFunction.FROM_CURRENT);
                } else {
                    StringBuilder sb = new StringBuilder();
                    for (Set_statement_valueContext val : action.value) {
                        sb.append(getFullCtxText(val)).append(", ");
                    }
                    sb.setLength(sb.length() - 2);
                    function.addConfiguration(par, sb.toString());
                }
            }
        }

        // Parsing the function definition and adding its result context for analysis.
        // Adding contexts of function arguments for analysis.
        List<Character_stringContext> funcContent = funcDef.character_string();
        if ("SQL".equalsIgnoreCase(language) && funcContent.size() == 1) {
            String def;
            TerminalNode codeStart = funcContent.get(0).Character_String_Literal();
            if (codeStart != null) {
                // TODO support special escaping schemes (maybe in the util itself)
                def = PgDiffUtils.unquoteQuotedString(codeStart.getText());
            } else {
                List<TerminalNode> dollarText = funcContent.get(0).Text_between_Dollar();
                codeStart = dollarText.get(0);
                def = dollarText.stream()
                        .map(TerminalNode::getText)
                        .collect(Collectors.joining());
            }

            AntlrParser.submitSqlCtxToAnalyze(def, errors,
                    codeStart.getSymbol().getStartIndex(),
                    codeStart.getSymbol().getLine() - 1,
                    codeStart.getSymbol().getCharPositionInLine(),
                    "function definition of " + function.getBareName(),
                    ctx -> db.addAnalysisLauncher(new FuncProcAnalysisLauncher(
                            function, ctx, funcArgs)),
                    antlrTasks);
        }

        With_storage_parameterContext storage = params.with_storage_parameter();
        if (storage != null) {
            for (Storage_parameter_optionContext option : storage.storage_parameter().storage_parameter_option()) {
                if ("isStrict".equalsIgnoreCase(option.getText())) {
                    function.setStrict(true);
                } else if ("isCachable".equalsIgnoreCase(option.getText())) {
                    function.setVolatileType("IMMUTABLE");
                }
            }
        }

        function.setLanguageCost(language, cost);
    }

    /**
     * Returns a list of pairs, each of which contains the name of the argument
     * and its full type name in GenericColumn object (typeSchema, typeName, DbObjType.TYPE).
     */
    private List<Pair<String, GenericColumn>> fillArguments(AbstractPgFunction function) {
        List<Pair<String, GenericColumn>> funcArgs = new ArrayList<>();
        for (Function_argumentsContext argument : ctx.function_parameters()
                .function_args().function_arguments()) {
            String argName = argument.argname != null ? argument.argname.getText() : null;
            String typeSchema = ApgdiffConsts.PG_CATALOG;
            String typeName;

            Schema_qualified_name_nontypeContext typeQname = argument.argtype_data.predefined_type()
                    .schema_qualified_name_nontype();
            if (typeQname != null) {
                if (typeQname.schema != null) {
                    typeSchema = typeQname.schema.getText();
                }
                typeName = typeQname.identifier_nontype().getText();
            } else {
                typeName = getFullCtxText(argument.argtype_data);
            }

            Argument arg = new Argument(argument.arg_mode != null ? argument.arg_mode.getText() : null,
                    argName, getTypeName(argument.argtype_data));
            addPgTypeDepcy(argument.argtype_data, function);

            if (argument.function_def_value() != null) {
                arg.setDefaultExpression(getFullCtxText(argument.function_def_value().def_value));

                db.addAnalysisLauncher(new FuncProcAnalysisLauncher(
                        function, argument.function_def_value().def_value));
            }

            function.addArgument(arg);
            funcArgs.add(new Pair<>(argName, new GenericColumn(typeSchema, typeName, DbObjType.TYPE)));
        }
        return funcArgs;
    }

    @Override
    protected Pair<StatementActions, GenericColumn> getActionAndObjForStmtAction() {
        List<IdentifierContext> ids = ctx.function_parameters().name.identifier();
        return new Pair<>(StatementActions.CREATE, new GenericColumn(
                QNameParser.getSchemaName(ids), QNameParser.getFirstNameCtx(ids).getText(),
                ctx.PROCEDURE() != null ? DbObjType.PROCEDURE : DbObjType.FUNCTION));
    }
}