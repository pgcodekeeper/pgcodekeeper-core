package cz.startnet.utils.pgdiff.parsers.antlr.statements.mssql;

import java.util.List;

import cz.startnet.utils.pgdiff.parsers.antlr.QNameParser;
import cz.startnet.utils.pgdiff.parsers.antlr.TSQLParser.Create_sequenceContext;
import cz.startnet.utils.pgdiff.parsers.antlr.TSQLParser.IdContext;
import cz.startnet.utils.pgdiff.parsers.antlr.TSQLParser.Sequence_bodyContext;
import cz.startnet.utils.pgdiff.parsers.antlr.statements.ParserAbstract;
import cz.startnet.utils.pgdiff.schema.MsSequence;
import cz.startnet.utils.pgdiff.schema.PgDatabase;
import cz.startnet.utils.pgdiff.schema.PgStatement;

public class CreateMsSequence extends ParserAbstract {

    private final Create_sequenceContext ctx;

    public CreateMsSequence(Create_sequenceContext ctx, PgDatabase db) {
        super(db);
        this.ctx = ctx;
    }

    @Override
    public PgStatement getObject() {
        List<IdContext> ids = ctx.simple_name().id();
        MsSequence sequence = new MsSequence(QNameParser.getFirstName(ids), getFullCtxText(ctx.getParent()));
        fillSequence(sequence, ctx.sequence_body());
        getSchemaSafe(ids, db.getDefaultSchema()).addSequence(sequence);
        return sequence;
    }

    private void fillSequence(MsSequence sequence, List<Sequence_bodyContext> list) {
        long inc = 1;
        Long maxValue = null;
        Long minValue = null;
        for (Sequence_bodyContext body : list) {
            if (body.data_type() != null) {
                sequence.setDataType(body.data_type().getText().toLowerCase());
            } else if (body.start_val != null) {
                sequence.setStartWith(body.start_val.getText());
            } else if (body.cache_val != null) {
                sequence.setCache(body.cache_val.getText());
            } else if (body.incr != null) {
                inc = Long.parseLong(body.incr.getText());
            } else if (body.maxval != null) {
                maxValue = Long.parseLong(body.maxval.getText());
            } else if (body.minval != null) {
                minValue = Long.parseLong(body.minval.getText());
            }  else if (body.cycle_val != null) {
                sequence.setCycle(body.cycle_true == null);
            }
        }

        sequence.setMinMaxInc(inc, maxValue, minValue);
    }
}