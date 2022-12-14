package ru.taximaxim.codekeeper.core.parsers.antlr.statements;

import java.util.List;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;

import ru.taximaxim.codekeeper.core.model.difftree.DbObjType;
import ru.taximaxim.codekeeper.core.parsers.antlr.QNameParser;
import ru.taximaxim.codekeeper.core.parsers.antlr.SQLParser.Alter_view_actionContext;
import ru.taximaxim.codekeeper.core.parsers.antlr.SQLParser.Alter_view_statementContext;
import ru.taximaxim.codekeeper.core.parsers.antlr.SQLParser.VexContext;
import ru.taximaxim.codekeeper.core.parsers.antlr.expr.launcher.VexAnalysisLauncher;
import ru.taximaxim.codekeeper.core.schema.AbstractSchema;
import ru.taximaxim.codekeeper.core.schema.PgDatabase;
import ru.taximaxim.codekeeper.core.schema.PgView;

public class AlterView extends ParserAbstract {

    private final Alter_view_statementContext ctx;
    private final CommonTokenStream stream;

    public AlterView(Alter_view_statementContext ctx, PgDatabase db, CommonTokenStream stream) {
        super(db);
        this.ctx = ctx;
        this.stream = stream;
    }

    @Override
    public void parseObject() {
        List<ParserRuleContext> ids = getIdentifiers(ctx.name);
        PgView dbView = (PgView) getSafe(AbstractSchema::getView,
                getSchemaSafe(ids), QNameParser.getFirstNameCtx(ids));
        Alter_view_actionContext action = ctx.alter_view_action();
        if (action.set_def_column() != null) {
            VexContext exp = action.set_def_column().vex();
            doSafe((s,o) -> {
                s.addColumnDefaultValue(getFullCtxText(action.column_name), getExpressionText(exp, stream));
                db.addAnalysisLauncher(new VexAnalysisLauncher(s, exp, fileName));
            }, dbView, null);
        }
        if (action.drop_def() != null) {
            doSafe(PgView::removeColumnDefaultValue, dbView, getFullCtxText(action.column_name));
        }

        addObjReference(ids, DbObjType.VIEW, ACTION_ALTER);
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_ALTER, DbObjType.VIEW, getIdentifiers(ctx.name));
    }
}
