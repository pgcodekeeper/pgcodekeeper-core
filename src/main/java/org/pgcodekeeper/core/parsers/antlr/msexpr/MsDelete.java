/*******************************************************************************
 * Copyright 2017-2025 TAXTELECOM, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.pgcodekeeper.core.parsers.antlr.msexpr;

import org.pgcodekeeper.core.parsers.antlr.generated.TSQLParser.*;

import java.util.Collections;
import java.util.List;

/**
 * Microsoft SQL DELETE statement analyzer.
 * Processes DELETE statements including CTEs, FROM clauses, and WHERE conditions
 * to extract database object dependencies.
 */
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
