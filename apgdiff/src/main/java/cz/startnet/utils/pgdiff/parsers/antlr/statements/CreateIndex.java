package cz.startnet.utils.pgdiff.parsers.antlr.statements;

import java.util.List;

import cz.startnet.utils.pgdiff.parsers.antlr.QNameParser;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Create_index_statementContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.IdentifierContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Index_restContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.ParamContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Param_clauseContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Schema_qualified_nameContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Sort_specifierContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Value_expression_primaryContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.VexContext;
import cz.startnet.utils.pgdiff.parsers.antlr.expr.ValueExpr;
import cz.startnet.utils.pgdiff.parsers.antlr.rulectx.Vex;
import cz.startnet.utils.pgdiff.schema.GenericColumn;
import cz.startnet.utils.pgdiff.schema.PgDatabase;
import cz.startnet.utils.pgdiff.schema.PgIndex;
import cz.startnet.utils.pgdiff.schema.PgSchema;
import cz.startnet.utils.pgdiff.schema.PgStatement;
import ru.taximaxim.codekeeper.apgdiff.model.difftree.DbObjType;

public class CreateIndex extends ParserAbstract {
    private final Create_index_statementContext ctx;
    private final String tablespace;

    public CreateIndex(Create_index_statementContext ctx, PgDatabase db, String tablespace) {
        super(db);
        this.ctx = ctx;
        this.tablespace = tablespace;
    }

    @Override
    public PgStatement getObject() {
        List<IdentifierContext> ids = ctx.name.identifier();
        PgSchema schema = getSchemaSafe(ids, db.getDefaultSchema());
        String name = QNameParser.getFirstName(ids);
        PgIndex ind = new PgIndex(name != null ? name : "", getFullCtxText(ctx.getParent()));
        ind.setTableName(QNameParser.getFirstName(ctx.table_name.identifier()));
        ind.setDefinition(parseIndex(ctx.index_rest(), tablespace, schema.getName(), ind));
        ind.setUnique(ctx.UNIQUE() != null);
        if (name != null) {
            getSafe(schema::getTable,
                    QNameParser.getFirstNameCtx(ctx.table_name.identifier())).addIndex(ind);
        }

        ind.addDep(new GenericColumn(schema.getName(), ind.getTableName(), DbObjType.TABLE));

        Param_clauseContext params = ctx.index_rest().index_sort().param_clause();
        if (params != null) {
            for (ParamContext param: params.param()){
                VexContext exp = param.value;
                if (exp != null){
                    ValueExpr vex = new ValueExpr(schema.getName());
                    vex.analyze(new Vex(exp));
                    ind.addAllDeps(vex.getDepcies());
                }
            }
        }

        // Костыль, т.к нужно улучшить парсер для vex в планевычитки колонок
        for (Sort_specifierContext sort_ctx : ctx.index_rest().index_sort().sort_specifier_list().sort_specifier()){
            Value_expression_primaryContext vexPrimary = sort_ctx.key.value_expression_primary();
            if (vexPrimary != null) {
                Schema_qualified_nameContext colName = vexPrimary.schema_qualified_name();
                if (colName != null) {
                    ind.addDep(new GenericColumn(schema.getName(), ind.getTableName(),
                            colName.getText(), DbObjType.COLUMN));
                }
            }
        }
        return ind;
    }


    public static String parseIndex(Index_restContext rest, String tablespace,
            String schemaName, PgIndex ind){
        Param_clauseContext params = rest.index_sort().param_clause();
        if (params != null) {
            for (ParamContext param : params.param()) {
                ValueExpr vex = new ValueExpr(schemaName);
                vex.analyze(new Vex(param.value));
                ind.addAllDeps(vex.getDepcies());
            }
        }
        StringBuilder sb = new StringBuilder();
        sb.append(ParserAbstract.getFullCtxText(rest.index_sort()));
        if (rest.table_space() != null){
            sb.append(' ').append(getFullCtxText(rest.table_space()));
        } else if (tablespace != null) {
            sb.append(" TABLESPACE ").append(tablespace);
        }
        if (rest.index_where() != null){
            ValueExpr vex = new ValueExpr(schemaName);
            vex.analyze(new Vex(rest.index_where().vex()));
            ind.addAllDeps(vex.getDepcies());
            sb.append(' ').append(ParserAbstract.getFullCtxText(rest.index_where()));
        }
        return sb.toString();
    }
}
