package cz.startnet.utils.pgdiff.parsers.antlr.statements;

import java.util.List;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;

import cz.startnet.utils.pgdiff.DangerStatement;
import cz.startnet.utils.pgdiff.loader.QueryLocation;
import cz.startnet.utils.pgdiff.parsers.antlr.QNameParser;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.IdentifierContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Update_stmt_for_psqlContext;
import cz.startnet.utils.pgdiff.schema.GenericColumn;
import cz.startnet.utils.pgdiff.schema.PgDatabase;
import cz.startnet.utils.pgdiff.schema.PgObjLocation;
import cz.startnet.utils.pgdiff.schema.StatementActions;
import ru.taximaxim.codekeeper.apgdiff.model.difftree.DbObjType;

public class UpdateStatement extends ParserAbstract {

    private final Update_stmt_for_psqlContext ctx;

    public UpdateStatement(Update_stmt_for_psqlContext ctx, PgDatabase db) {
        super(db);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        List<IdentifierContext> ids = ctx.update_table_name.identifier();
        PgObjLocation loc = addObjReference(ids, DbObjType.TABLE, StatementActions.UPDATE);
        loc.setWarning(DangerStatement.UPDATE);
    }

    @Override
    protected QueryLocation fillQueryLocation(ParserRuleContext ctx, CommonTokenStream tokenStream) {
        QueryLocation loc = super.fillQueryLocation(ctx, tokenStream);
        loc.setWarning(DangerStatement.UPDATE);
        return loc;
    }

    @Override
    protected void fillDescrObj() {
        action = StatementActions.UPDATE;
        List<IdentifierContext> ids = ctx.update_table_name.identifier();
        descrObj = new GenericColumn(QNameParser.getSchemaName(ids),
                QNameParser.getFirstNameCtx(ids).getText(), DbObjType.TABLE);
    }
}
