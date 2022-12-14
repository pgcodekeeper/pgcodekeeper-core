package ru.taximaxim.codekeeper.core.parsers.antlr.msexpr;

import java.util.Collections;
import java.util.List;

import ru.taximaxim.codekeeper.core.parsers.antlr.TSQLParser.Delete_statementContext;
import ru.taximaxim.codekeeper.core.parsers.antlr.TSQLParser.ExpressionContext;
import ru.taximaxim.codekeeper.core.parsers.antlr.TSQLParser.From_itemContext;
import ru.taximaxim.codekeeper.core.parsers.antlr.TSQLParser.Qualified_nameContext;
import ru.taximaxim.codekeeper.core.parsers.antlr.TSQLParser.Search_conditionContext;
import ru.taximaxim.codekeeper.core.parsers.antlr.TSQLParser.Top_countContext;
import ru.taximaxim.codekeeper.core.parsers.antlr.TSQLParser.With_expressionContext;

public class MsDelete extends MsAbstractExprWithNmspc<Delete_statementContext> {

    protected MsDelete(MsAbstractExpr parent) {
        super(parent);
    }

    @Override
    public List<String> analyze(Delete_statementContext delete) {
        With_expressionContext with = delete.with_expression();
        if (with != null) {
            analyzeCte(with);
        }

        Qualified_nameContext tableName = delete.qualified_name();

        MsSelect select = new MsSelect(this);
        for (From_itemContext item : delete.from_item()) {
            select.from(item);
        }

        boolean isAlias = false;

        if (tableName != null && tableName.schema == null) {
            isAlias = select.findReference(null, tableName.name.getText()) != null;
        }

        if (tableName != null && !isAlias) {
            addNameReference(tableName, null);
        }

        Top_countContext top = delete.top_count();
        if (top != null) {
            ExpressionContext exp = top.expression();
            if (exp != null) {
                new MsValueExpr(select).analyze(exp);
            }
        }

        Search_conditionContext search = delete.search_condition();
        if (search != null) {
            new MsValueExpr(select).search(search);
        }

        return Collections.emptyList();
    }
}
