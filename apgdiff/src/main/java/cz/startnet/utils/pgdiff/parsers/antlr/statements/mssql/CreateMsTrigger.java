package cz.startnet.utils.pgdiff.parsers.antlr.statements.mssql;

import java.util.Arrays;
import java.util.List;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;

import cz.startnet.utils.pgdiff.parsers.antlr.TSQLParser.Batch_statementContext;
import cz.startnet.utils.pgdiff.parsers.antlr.TSQLParser.Create_or_alter_triggerContext;
import cz.startnet.utils.pgdiff.parsers.antlr.TSQLParser.IdContext;
import cz.startnet.utils.pgdiff.parsers.antlr.msexpr.MsSqlClauses;
import cz.startnet.utils.pgdiff.parsers.antlr.statements.ParserAbstract;
import cz.startnet.utils.pgdiff.schema.AbstractSchema;
import cz.startnet.utils.pgdiff.schema.GenericColumn;
import cz.startnet.utils.pgdiff.schema.IStatementContainer;
import cz.startnet.utils.pgdiff.schema.MsTrigger;
import cz.startnet.utils.pgdiff.schema.PgDatabase;
import cz.startnet.utils.pgdiff.schema.PgObjLocation;
import cz.startnet.utils.pgdiff.schema.PgStatement;
import ru.taximaxim.codekeeper.apgdiff.model.difftree.DbObjType;
import ru.taximaxim.codekeeper.apgdiff.utils.Pair;

public class CreateMsTrigger extends BatchContextProcessor {

    private final Create_or_alter_triggerContext ctx;

    private final boolean ansiNulls;
    private final boolean quotedIdentifier;

    public CreateMsTrigger(Batch_statementContext ctx, PgDatabase db,
            boolean ansiNulls, boolean quotedIdentifier, CommonTokenStream stream) {
        super(db, ctx, stream);
        this.ctx = ctx.batch_statement_body().create_or_alter_trigger();
        this.ansiNulls = ansiNulls;
        this.quotedIdentifier = quotedIdentifier;
    }

    @Override
    protected ParserRuleContext getDelimiterCtx() {
        return ctx.table_name;
    }

    @Override
    public void parseObject() {
        IdContext schemaCtx = ctx.trigger_name.schema;
        if (schemaCtx == null) {
            schemaCtx = ctx.table_name.schema;
        }
        List<IdContext> ids = Arrays.asList(schemaCtx, ctx.table_name.name);
        addObjReference(ids, DbObjType.TABLE, ACTION_NONE);
        getObject(getSchemaSafe(ids), false);
    }

    public MsTrigger getObject(AbstractSchema schema, boolean isJdbc) {
        IdContext schemaCtx = ctx.trigger_name.schema;
        if (schemaCtx == null) {
            schemaCtx = ctx.table_name.schema;
        }
        IdContext tableNameCtx = ctx.table_name.name;
        IdContext nameCtx = ctx.trigger_name.name;

        MsTrigger trigger = new MsTrigger(nameCtx.getText());
        trigger.setAnsiNulls(ansiNulls);
        trigger.setQuotedIdentified(quotedIdentifier);
        setSourceParts(trigger);

        String schemaName;
        if (schema != null) {
            schemaName = schema.getName();
        } else {
            List<IdContext> ids = Arrays.asList(schemaCtx, tableNameCtx);
            schemaName = getSchemaNameSafe(ids);
            addObjReference(ids, DbObjType.TABLE, ACTION_NONE);
        }

        MsSqlClauses clauses;
        if (db.getArguments().isEnableFunctionBodiesDependencies()) {
            clauses = new MsSqlClauses(schemaName);
        } else {
            clauses = new MsSqlClauses(schemaName, DbObjType.FUNCTION, DbObjType.PROCEDURE);
        }
        clauses.analyze(ctx.sql_clauses());
        trigger.addAllDeps(clauses.getDepcies());

        IStatementContainer cont = getSafe(AbstractSchema::getStatementContainer,
                schema, tableNameCtx);

        if (isJdbc && schema != null) {
            cont.addTrigger(trigger);
        } else {
            addSafe((PgStatement) cont, trigger,
                    Arrays.asList(schemaCtx, tableNameCtx, nameCtx));
        }
        return trigger;
    }

    @Override
    protected PgObjLocation fillQueryLocation(ParserRuleContext ctx) {
        PgObjLocation loc = new PgObjLocation(getStmtAction(ctx), ctx,
                ParserAbstract.getFullCtxTextWithHidden(ctx, stream));
        db.addToQueries(fileName, loc);
        return loc;
    }

    @Override
    protected Pair<String, GenericColumn> getActionAndObjForStmtAction() {
        IdContext schemaCtx = ctx.trigger_name.schema;
        if (schemaCtx == null) {
            schemaCtx = ctx.table_name.schema;
        }
        return new Pair<>(ACTION_CREATE, new GenericColumn(schemaCtx.getText(),
                ctx.table_name.name.getText(), ctx.trigger_name.name.getText(), DbObjType.TRIGGER));
    }
}
