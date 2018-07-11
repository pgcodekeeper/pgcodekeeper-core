package cz.startnet.utils.pgdiff.parsers.antlr.statements.mssql;

import java.util.List;

import cz.startnet.utils.pgdiff.parsers.antlr.QNameParser;
import cz.startnet.utils.pgdiff.parsers.antlr.TSQLParser.Create_or_alter_viewContext;
import cz.startnet.utils.pgdiff.parsers.antlr.TSQLParser.IdContext;
import cz.startnet.utils.pgdiff.parsers.antlr.TSQLParser.Select_statementContext;
import cz.startnet.utils.pgdiff.parsers.antlr.TSQLParser.View_attributeContext;
import cz.startnet.utils.pgdiff.parsers.antlr.statements.ParserAbstract;
import cz.startnet.utils.pgdiff.schema.MsView;
import cz.startnet.utils.pgdiff.schema.PgDatabase;
import cz.startnet.utils.pgdiff.schema.PgSchema;
import cz.startnet.utils.pgdiff.schema.PgStatement;

public class CreateMsView extends ParserAbstract {

    private static final String CHECK_OPTION = "check_option";

    private final Create_or_alter_viewContext ctx;

    public CreateMsView(Create_or_alter_viewContext ctx, PgDatabase db) {
        super(db);
        this.ctx = ctx;
    }

    @Override
    public PgStatement getObject() {
        List<IdContext> ids = ctx.simple_name().id();
        PgSchema schema = getSchemaSafe(ids, db.getDefaultSchema());
        IdContext name = QNameParser.getFirstNameCtx(ids);
        MsView view = new MsView(name.getText(), getFullCtxText(ctx.getParent()));

        Select_statementContext vQuery = ctx.select_statement();
        if (vQuery != null) {
            // TODO select analyze
            view.setQuery(getFullCtxText(vQuery));
        }

        if (ctx.column_name_list() != null) {
            for (IdContext column : ctx.column_name_list().id()) {
                view.addColumnName(ParserAbstract.getFullCtxText(column));
            }
        }

        List<View_attributeContext> options = ctx.view_attribute();
        if (options != null){
            for (View_attributeContext option: options) {
                ParserAbstract.fillOptionParams("", option.getText() , false, view::addOption);
            }
        }

        if (ctx.with_check_option() != null){
            view.addOption(CHECK_OPTION, "");
        }

        schema.addView(view);
        return view;
    }
}
