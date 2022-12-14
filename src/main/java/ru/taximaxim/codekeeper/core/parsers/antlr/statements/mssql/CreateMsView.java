package ru.taximaxim.codekeeper.core.parsers.antlr.statements.mssql;

import java.util.Arrays;
import java.util.List;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;

import ru.taximaxim.codekeeper.core.model.difftree.DbObjType;
import ru.taximaxim.codekeeper.core.parsers.antlr.TSQLParser.Batch_statementContext;
import ru.taximaxim.codekeeper.core.parsers.antlr.TSQLParser.Create_or_alter_viewContext;
import ru.taximaxim.codekeeper.core.parsers.antlr.TSQLParser.IdContext;
import ru.taximaxim.codekeeper.core.parsers.antlr.TSQLParser.Qualified_nameContext;
import ru.taximaxim.codekeeper.core.parsers.antlr.TSQLParser.Select_statementContext;
import ru.taximaxim.codekeeper.core.parsers.antlr.expr.launcher.MsViewAnalysisLauncher;
import ru.taximaxim.codekeeper.core.schema.AbstractSchema;
import ru.taximaxim.codekeeper.core.schema.MsView;
import ru.taximaxim.codekeeper.core.schema.PgDatabase;

public class CreateMsView extends BatchContextProcessor {

    private final Create_or_alter_viewContext ctx;

    private final boolean ansiNulls;
    private final boolean quotedIdentifier;

    public CreateMsView(Batch_statementContext ctx, PgDatabase db,
            boolean ansiNulls, boolean quotedIdentifier, CommonTokenStream stream) {
        super(db, ctx, stream);
        this.ctx = ctx.batch_statement_body().create_or_alter_view();
        this.ansiNulls = ansiNulls;
        this.quotedIdentifier = quotedIdentifier;
    }

    public CreateMsView(Create_or_alter_viewContext ctx, PgDatabase db,
            boolean ansiNulls, boolean quotedIdentifier, CommonTokenStream stream) {
        super(db, ctx.getParent(), stream);
        this.ctx = ctx;
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

    public MsView getObject(AbstractSchema schema, boolean isJdbc) {
        IdContext nameCtx = ctx.qualified_name().name;
        List<ParserRuleContext> ids = Arrays.asList(ctx.qualified_name().schema, nameCtx);
        MsView view = new MsView(nameCtx.getText());
        view.setAnsiNulls(ansiNulls);
        view.setQuotedIdentified(quotedIdentifier);
        setSourceParts(view);

        Select_statementContext vQuery = ctx.select_statement();
        if (vQuery != null) {
            db.addAnalysisLauncher(new MsViewAnalysisLauncher(view, vQuery, fileName));
        }

        if (isJdbc && schema != null) {
            schema.addView(view);
        } else {
            addSafe(schema, view, ids);
        }
        return view;
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_CREATE, DbObjType.VIEW, ctx.qualified_name());
    }
}
