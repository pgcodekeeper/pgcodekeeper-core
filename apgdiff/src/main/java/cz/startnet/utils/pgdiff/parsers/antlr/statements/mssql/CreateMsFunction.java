package cz.startnet.utils.pgdiff.parsers.antlr.statements.mssql;

import cz.startnet.utils.pgdiff.parsers.antlr.TSQLParser.Create_or_alter_functionContext;
import cz.startnet.utils.pgdiff.parsers.antlr.TSQLParser.Func_returnContext;
import cz.startnet.utils.pgdiff.parsers.antlr.TSQLParser.IdContext;
import cz.startnet.utils.pgdiff.parsers.antlr.TSQLParser.Procedure_paramContext;
import cz.startnet.utils.pgdiff.parsers.antlr.statements.ParserAbstract;
import cz.startnet.utils.pgdiff.schema.MsFunction;
import cz.startnet.utils.pgdiff.schema.PgDatabase;
import cz.startnet.utils.pgdiff.schema.PgFunction.Argument;
import cz.startnet.utils.pgdiff.schema.PgSchema;
import cz.startnet.utils.pgdiff.schema.PgStatement;

public class CreateMsFunction extends ParserAbstract {

    private final Create_or_alter_functionContext ctx;

    private final boolean ansiNulls;
    private final boolean quotedIdentifier;

    public CreateMsFunction(Create_or_alter_functionContext ctx, PgDatabase db, boolean ansiNulls, boolean quotedIdentifier) {
        super(db);
        this.ctx = ctx;
        this.ansiNulls = ansiNulls;
        this.quotedIdentifier = quotedIdentifier;
    }

    @Override
    public PgStatement getObject() {
        IdContext schemaCtx = ctx.func_proc_name().schema;
        PgSchema schema = schemaCtx == null ? db.getDefaultSchema() : getSafe(db::getSchema, schemaCtx);
        MsFunction function = new MsFunction(ctx.func_proc_name().procedure.getText(), getFullCtxText(ctx.getParent()));
        function.setAnsiNulls(ansiNulls);
        function.setQuotedIdentified(quotedIdentifier);
        fillArguments(function);
        function.setBody(db.getArguments(), getFullCtxText(ctx.func_body()));
        Func_returnContext returns = ctx.func_return();
        function.setReturns(getFullCtxText(returns));
        schema.addFunction(function);
        return function;
    }

    private void fillArguments(MsFunction function) {
        for (Procedure_paramContext argument : ctx.procedure_param()) {
            Argument arg = function.new MsArgument(
                    argument.arg_mode != null ? argument.arg_mode.getText() : null,
                            argument.name.getText(), getFullCtxText(argument.data_type()));

            if (argument.default_val != null) {
                arg.setDefaultExpression(getFullCtxText(argument.default_val));
            }

            function.addArgument(arg);
        }
    }
}