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

import java.util.*;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.database.ms.parser.generated.TSQLParser.*;
import org.pgcodekeeper.core.database.ms.parser.launcher.MsFuncProcTrigAnalysisLauncher;
import org.pgcodekeeper.core.database.ms.schema.*;
import org.pgcodekeeper.core.settings.ISettings;

/**
 * Parser for Microsoft SQL CREATE PROCEDURE statements.
 * Handles both regular T-SQL procedures and CLR procedures with support for
 * arguments, procedure options, and proper analysis setup.
 */
public final class MsCreateProcedure extends MsBatchContextProcessor {

    private final Create_or_alter_procedureContext ctx;

    private final boolean ansiNulls;
    private final boolean quotedIdentifier;

    /**
     * Creates a parser for Microsoft SQL CREATE PROCEDURE statements.
     *
     * @param ctx              the batch statement context containing the procedure definition
     * @param db               the Microsoft SQL database schema being processed
     * @param ansiNulls        the ANSI_NULLS setting for the procedure
     * @param quotedIdentifier the QUOTED_IDENTIFIER setting for the procedure
     * @param stream           the token stream for source code processing
     * @param settings         parsing configuration settings
     */
    public MsCreateProcedure(Batch_statementContext ctx, MsDatabase db,
                             boolean ansiNulls, boolean quotedIdentifier, CommonTokenStream stream, ISettings settings) {
        super(db, ctx, stream, settings);
        this.ctx = ctx.batch_statement_body().create_or_alter_procedure();
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
     * Creates and configures the procedure object from the parse context.
     * Handles both CLR external procedures and regular T-SQL procedures with appropriate analysis setup.
     *
     * @param schema the schema to add the procedure to
     * @param isJdbc whether this is being parsed in JDBC mode
     * @return the created procedure object
     */
    public AbstractFunction getObject(AbstractSchema schema, boolean isJdbc) {
        IdContext nameCtx = ctx.qualified_name().name;
        List<ParserRuleContext> ids = Arrays.asList(ctx.qualified_name().schema, nameCtx);
        if (ctx.proc_body().EXTERNAL() != null) {
            Assembly_specifierContext assemblyCtx = ctx.proc_body().assembly_specifier();
            String assembly = assemblyCtx.assembly_name.getText();
            String assemblyClass = assemblyCtx.class_name.getText();
            String assemblyMethod = assemblyCtx.method_name.getText();
            MsClrProcedure procedure = new MsClrProcedure(nameCtx.getText(),
                    assembly, assemblyClass, assemblyMethod);

            addDepSafe(procedure, Collections.singletonList(assemblyCtx.assembly_name), DbObjType.ASSEMBLY);
            fillArguments(procedure);

            for (Procedure_optionContext option : ctx.procedure_option()) {
                procedure.addOption(getFullCtxText(option));
            }
            if (isJdbc) {
                schema.addFunction(procedure);
            } else {
                addSafe(schema, procedure, ids);
            }
            return procedure;
        }

        MsProcedure procedure = new MsProcedure(nameCtx.getText());
        procedure.setAnsiNulls(ansiNulls);
        procedure.setQuotedIdentified(quotedIdentifier);

        fillArguments(procedure);
        setSourceParts(procedure);

        db.addAnalysisLauncher(new MsFuncProcTrigAnalysisLauncher(procedure,
                ctx.proc_body().sql_clauses(), fileName, settings.isEnableFunctionBodiesDependencies()));

        if (isJdbc && schema != null) {
            schema.addFunction(procedure);
        } else {
            addSafe(schema, procedure, ids);
        }
        return procedure;
    }

    private void fillArguments(AbstractFunction function) {
        for (Procedure_paramContext argument : ctx.procedure_param()) {
            Argument arg = new Argument(parseArgMode(argument.arg_mode()),
                    argument.name.getText(), getFullCtxText(argument.data_type()));
            arg.setReadOnly(argument.READONLY() != null);
            addTypeDepcy(argument.data_type(), function);
            if (argument.default_val != null) {
                arg.setDefaultExpression(getFullCtxText(argument.default_val));
            }

            function.addArgument(arg);
        }
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_CREATE, DbObjType.PROCEDURE, ctx.qualified_name());
    }
}