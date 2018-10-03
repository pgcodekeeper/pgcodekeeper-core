package cz.startnet.utils.pgdiff.parsers.antlr.statements.mssql;

import cz.startnet.utils.pgdiff.parsers.antlr.TSQLParser.Create_or_alter_functionContext;
import cz.startnet.utils.pgdiff.parsers.antlr.TSQLParser.IdContext;
import cz.startnet.utils.pgdiff.parsers.antlr.TSQLParser.Procedure_paramContext;
import cz.startnet.utils.pgdiff.parsers.antlr.statements.ParserAbstract;
import cz.startnet.utils.pgdiff.schema.AbstractFunction;
import cz.startnet.utils.pgdiff.schema.AbstractSchema;
import cz.startnet.utils.pgdiff.schema.Argument;
import cz.startnet.utils.pgdiff.schema.MsFunction;
import cz.startnet.utils.pgdiff.schema.PgDatabase;

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
    public MsFunction getObject() {
        IdContext schemaCtx = ctx.func_proc_name().schema;
        AbstractSchema schema = schemaCtx == null ? db.getDefaultSchema() : getSafe(db::getSchema, schemaCtx);
        return getObject(schema);
    }

    public MsFunction getObject(AbstractSchema schema) {
        MsFunction function = new MsFunction(ctx.func_proc_name().procedure.getText(), getFullCtxText(ctx.getParent().getParent()));
        if (ctx.func_body().func_body_return().EXTERNAL() != null) {
            function.setAnsiNulls(false);
            function.setQuotedIdentified(false);
            function.setCLR(true);
        } else {
            function.setAnsiNulls(ansiNulls);
            function.setQuotedIdentified(quotedIdentifier);
        }
        fillArguments(function);
        function.setBody(db.getArguments(), getFullCtxText(ctx.func_body()));
        String returns = getFullCtxText(ctx.func_return());
        function.setReturns(db.getArguments().isKeepNewlines() ? returns : returns.replace("\r", ""));
        schema.addFunction(function);
        return function;
    }

    private void fillArguments(AbstractFunction function) {
        for (Procedure_paramContext argument : ctx.procedure_param()) {
            Argument arg = new Argument(
                    argument.arg_mode != null ? argument.arg_mode.getText() : null,
                            argument.name.getText(), getFullCtxText(argument.data_type()));

            if (argument.default_val != null) {
                arg.setDefaultExpression(getFullCtxText(argument.default_val));
            }

            function.addArgument(arg);
        }
    }
}
