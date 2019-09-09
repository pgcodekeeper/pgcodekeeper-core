package cz.startnet.utils.pgdiff.parsers.antlr.statements;

import java.util.List;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;

import cz.startnet.utils.pgdiff.DangerStatement;
import cz.startnet.utils.pgdiff.loader.QueryLocation;
import cz.startnet.utils.pgdiff.parsers.antlr.QNameParser;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Alter_sequence_statementContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.IdentifierContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Schema_alterContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Sequence_bodyContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Tokens_nonreserved_except_function_typeContext;
import cz.startnet.utils.pgdiff.schema.AbstractSchema;
import cz.startnet.utils.pgdiff.schema.GenericColumn;
import cz.startnet.utils.pgdiff.schema.PgDatabase;
import cz.startnet.utils.pgdiff.schema.PgObjLocation;
import cz.startnet.utils.pgdiff.schema.PgSequence;
import cz.startnet.utils.pgdiff.schema.StatementActions;
import ru.taximaxim.codekeeper.apgdiff.model.difftree.DbObjType;

public class AlterSequence extends ParserAbstract {
    private final Alter_sequence_statementContext ctx;
    public AlterSequence(Alter_sequence_statementContext ctx, PgDatabase db) {
        super(db);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        List<IdentifierContext> ids = ctx.name.identifier();
        PgSequence sequence = (PgSequence) getSafe(AbstractSchema::getSequence,
                getSchemaSafe(ids), QNameParser.getFirstNameCtx(ids));

        PgObjLocation loc = addObjReference(ids, DbObjType.SEQUENCE, StatementActions.ALTER);

        for (Sequence_bodyContext seqbody : ctx.sequence_body()) {
            if (seqbody.OWNED() != null && seqbody.col_name != null) {
                List<IdentifierContext> col = seqbody.col_name.identifier();
                Tokens_nonreserved_except_function_typeContext word;
                if (col.size() != 1 || (word = col.get(0).tokens_nonreserved_except_function_type()) == null
                        || word.NONE() == null) {
                    doSafe(PgSequence::setOwnedBy, sequence, getFullCtxText(seqbody.col_name));
                }

                addObjReference(col, DbObjType.TABLE, StatementActions.NONE);
            }
        }

        if (!ctx.RESTART().isEmpty()) {
            loc.setWarning(DangerStatement.RESTART_WITH);
        }
    }

    @Override
    protected QueryLocation fillQueryLocation(ParserRuleContext ctx, CommonTokenStream tokenStream) {
        QueryLocation loc = super.fillQueryLocation(ctx, tokenStream);
        if (!((Schema_alterContext) ctx).alter_sequence_statement().RESTART().isEmpty()) {
            loc.setWarning(DangerStatement.RESTART_WITH);
        }
        return loc;
    }

    @Override
    protected void fillDescrObj() {
        action = StatementActions.CREATE;
        List<IdentifierContext> ids = ctx.name.identifier();
        descrObj = new GenericColumn(QNameParser.getSchemaName(ids),
                QNameParser.getFirstNameCtx(ids).getText(), DbObjType.SEQUENCE);
    }
}