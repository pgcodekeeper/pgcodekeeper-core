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

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.ms.launcher.MsFuncProcTrigAnalysisLauncher;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.*;
import org.pgcodekeeper.core.database.base.schema.AbstractFunction;
import org.pgcodekeeper.core.database.base.schema.AbstractSchema;
import org.pgcodekeeper.core.database.base.schema.Argument;
import org.pgcodekeeper.core.database.ms.schema.MsFunctionTypes;
import org.pgcodekeeper.core.database.ms.schema.MsClrFunction;
import org.pgcodekeeper.core.database.ms.schema.MsDatabase;
import org.pgcodekeeper.core.database.ms.schema.MsFunction;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Parser for Microsoft SQL CREATE FUNCTION statements.
 * Handles both regular T-SQL functions and CLR functions with support for
 * different function types (scalar, table-valued, multi-statement table-valued).
 */
public final class CreateMsFunction extends BatchContextProcessor {

    private final Create_or_alter_functionContext ctx;

    private final boolean ansiNulls;
    private final boolean quotedIdentifier;

    /**
     * Creates a parser for Microsoft SQL CREATE FUNCTION statements.
     *
     * @param ctx              the batch statement context containing the function definition
     * @param db               the Microsoft SQL database schema being processed
     * @param ansiNulls        the ANSI_NULLS setting for the function
     * @param quotedIdentifier the QUOTED_IDENTIFIER setting for the function
     * @param stream           the token stream for source code processing
     * @param settings         parsing configuration settings
     */
    public CreateMsFunction(Batch_statementContext ctx, MsDatabase db,
                            boolean ansiNulls, boolean quotedIdentifier, CommonTokenStream stream, ISettings settings) {
        super(db, ctx, stream, settings);
        this.ctx = ctx.batch_statement_body().create_or_alter_function();
        this.ansiNulls = ansiNulls;
        this.quotedIdentifier = quotedIdentifier;
    }

    @Override
    protected ParserRuleContext getDelimiterCtx() {
        return ctx.qualified_name();
    }

    @Override
    public void parseObject() {
        Qualified_nameContext qname = ctx.qualified_name();
        getObject(getSchemaSafe(Arrays.asList(qname.schema, qname.name)), false);
    }

    /**
     * Creates and configures the function object from the parse context.
     * Handles both CLR external functions and regular T-SQL functions with appropriate analysis setup.
     *
     * @param schema the schema to add the function to
     * @param isJdbc whether this is being parsed in JDBC mode
     * @return the created function object
     */
    public AbstractFunction getObject(AbstractSchema schema, boolean isJdbc) {
        IdContext nameCtx = ctx.qualified_name().name;
        List<ParserRuleContext> ids = Arrays.asList(ctx.qualified_name().schema, nameCtx);
        String name = nameCtx.getText();
        Func_bodyContext bodyRet = ctx.func_body();

        AbstractFunction f;
        if (bodyRet.EXTERNAL() != null) {
            Assembly_specifierContext assemblyCtx = bodyRet.assembly_specifier();
            String assembly = assemblyCtx.assembly_name.getText();
            String assemblyClass = assemblyCtx.class_name.getText();
            String assemblyMethod = assemblyCtx.method_name.getText();

            MsClrFunction func = new MsClrFunction(name, assembly,
                    assemblyClass, assemblyMethod);

            addDepSafe(func, Collections.singletonList(assemblyCtx.assembly_name), DbObjType.ASSEMBLY);

            for (Function_optionContext option : ctx.function_option()) {
                func.addOption(getFullCtxText(option));
            }

            func.setReturns(getFullCtxTextWithCheckNewLines(ctx.func_return()));

            Func_returnContext ret = ctx.func_return();
            if (ret.LOCAL_ID() != null) {
                func.setFuncType(MsFunctionTypes.MULTI);
            } else if (ret.data_type() == null) {
                func.setFuncType(MsFunctionTypes.TABLE);
            }

            f = func;
        } else {
            MsFunction func = new MsFunction(name);
            func.setAnsiNulls(ansiNulls);
            func.setQuotedIdentified(quotedIdentifier);
            setSourceParts(func);

            Select_statementContext select = bodyRet.select_statement();

            if (select != null) {
                db.addAnalysisLauncher(new MsFuncProcTrigAnalysisLauncher(
                        func, select, fileName, settings.isEnableFunctionBodiesDependencies()));
            } else {
                ExpressionContext exp = bodyRet.expression();
                if (exp != null) {
                    db.addAnalysisLauncher(new MsFuncProcTrigAnalysisLauncher(
                            func, exp, fileName, settings.isEnableFunctionBodiesDependencies()));
                }

                Sql_clausesContext clausesCtx = bodyRet.sql_clauses();
                if (clausesCtx != null) {
                    db.addAnalysisLauncher(new MsFuncProcTrigAnalysisLauncher(
                            func, clausesCtx, fileName, settings.isEnableFunctionBodiesDependencies()));
                }
            }

            Func_returnContext ret = ctx.func_return();
            if (ret.LOCAL_ID() != null) {
                func.setFuncType(MsFunctionTypes.MULTI);
            } else if (ret.data_type() == null) {
                func.setFuncType(MsFunctionTypes.TABLE);
            }

            f = func;
        }

        fillArguments(f);
        analyzeReturn(ctx.func_return(), f);

        if (isJdbc && schema != null) {
            schema.addFunction(f);
        } else {
            addSafe(schema, f, ids);
        }

        return f;
    }

    private void analyzeReturn(Func_returnContext ret, AbstractFunction function) {
        var typeCtx = ret.data_type();
        if (typeCtx != null) {
            addTypeDepcy(typeCtx, function);
            return;
        }

        Table_elementsContext elements = ret.table_elements();
        if (elements == null) {
            return;
        }

        for (Table_elementContext element : elements.table_element()) {
            Column_defContext colCtx = element.column_def();
            if (colCtx != null) {
                var dt = colCtx.data_type();
                if (dt != null) {
                    addTypeDepcy(dt, function);
                }
            }
        }
    }

    private void fillArguments(AbstractFunction function) {
        for (Procedure_paramContext argument : ctx.procedure_param()) {
            addTypeDepcy(argument.data_type(), function);

            Argument arg = new Argument(parseArgMode(argument.arg_mode()),
                    argument.name.getText(), getFullCtxText(argument.data_type()));
            arg.setReadOnly(argument.READONLY() != null);
            if (argument.default_val != null) {
                arg.setDefaultExpression(getFullCtxTextWithCheckNewLines(argument.default_val));
            }

            function.addArgument(arg);
        }
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_CREATE, DbObjType.FUNCTION, ctx.qualified_name());
    }
}
