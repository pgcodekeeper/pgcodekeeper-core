package cz.startnet.utils.pgdiff.parsers.antlr.statements;

import java.util.AbstractMap;
import java.util.List;

import cz.startnet.utils.pgdiff.parsers.antlr.QNameParser;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Create_index_statementContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.IdentifierContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Index_restContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Index_sortContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Index_whereContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Schema_qualified_nameContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Sort_specifierContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Value_expression_primaryContext;
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
        List<IdentifierContext> ids = ctx.table_name.identifier();
        PgSchema schema = getSchemaSafe(ids, db.getDefaultSchema());
        String name = ctx.name.getText();
        PgIndex ind = new PgIndex(name != null ? name : "", getFullCtxText(ctx.getParent()));
        ind.setTableName(QNameParser.getFirstName(ctx.table_name.identifier()));
        ind.setDefinition(parseIndex(ctx.index_rest(), tablespace, ind, db));
        ind.setUnique(ctx.UNIQUE() != null);
        if (name != null) {
            getSafe(schema::getTable,
                    QNameParser.getFirstNameCtx(ctx.table_name.identifier())).addIndex(ind);
        }

        ind.addDep(new GenericColumn(schema.getName(), ind.getTableName(), DbObjType.TABLE));

        parseIndexCols(ctx.index_rest().index_sort(), ind, schema);

        return ind;
    }

    private static String parseIndex(Index_restContext rest, String tablespace,
            PgIndex ind, PgDatabase db){
        StringBuilder sb = new StringBuilder();
        sb.append(ParserAbstract.getFullCtxText(rest.index_sort()));
        if (rest.table_space() != null){
            sb.append(' ').append(getFullCtxText(rest.table_space()));
        } else if (tablespace != null) {
            sb.append(" TABLESPACE ").append(tablespace);
        }
        if (rest.index_where() != null){
            Index_whereContext whereCtx = rest.index_where();
            db.getContextsForAnalyze().add(new AbstractMap.SimpleEntry<>(ind, whereCtx.vex()));
            sb.append(' ').append(ParserAbstract.getFullCtxText(whereCtx));
        }
        return sb.toString();
    }

    // Костыль, т.к нужно улучшить парсер для vex в планевычитки колонок
    public static void parseIndexCols(Index_sortContext indexSort, PgIndex ind, PgSchema schema) {
        for (Sort_specifierContext sort_ctx : indexSort.sort_specifier_list().sort_specifier()) {
            Value_expression_primaryContext vexPrimary = sort_ctx.key.value_expression_primary();
            if (vexPrimary != null) {
                Schema_qualified_nameContext colName = vexPrimary.schema_qualified_name();
                if (colName != null) {
                    ind.addDep(new GenericColumn(schema.getName(), ind.getTableName(),
                            colName.getText(), DbObjType.COLUMN));
                }
            }
            ind.addColumn(sort_ctx.key.getText());
        }
    }
}
