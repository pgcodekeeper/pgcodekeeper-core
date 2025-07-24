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
 * Microsoft SQL MERGE statement analyzer.
 * Processes MERGE statements including CTEs, target tables, source tables,
 * and merge conditions to extract database object dependencies.
 */
public class MsMerge extends MsAbstractExprWithNmspc<Merge_statementContext> {

    protected MsMerge(MsAbstractExpr parent) {
        super(parent);
    }

    @Override
    public List<String> analyze(Merge_statementContext merge) {
        With_expressionContext with = merge.with_expression();
        if (with != null) {
            analyzeCte(with);
        }

        Qualified_nameContext tableName = merge.qualified_name();
        if (tableName != null) {
            addNameReference(tableName, merge.as_table_alias());
        }

        MsSelect select = new MsSelect(this);

        for (From_itemContext item : merge.from_item()) {
            select.from(item);
        }

        MsValueExpr vex = new MsValueExpr(select);

        ExpressionContext exp = merge.expression();
        if (exp != null) {
            vex.analyze(exp);
        }

        for (var when : merge.merge_when()) {
            analyzeWhen(select, vex, when);
        }

        return Collections.emptyList();
    }

    private void analyzeWhen(MsSelect select, MsValueExpr vex, Merge_whenContext when) {
        var search = when.search_condition();
        if (search != null) {
            vex.search(search);
        }

        var matched = when.merge_matched();
        var notMatched = when.merge_not_matched();
        if (notMatched != null) {
            Table_value_constructorContext tvc = notMatched.table_value_constructor();
            if (tvc != null) {
                for (Expression_listContext list : tvc.expression_list()) {
                    vex.expressionList(list);
                }
            }
        } else {
            for (Update_elemContext elem : matched.update_elem()) {
                ExpressionContext updateExpr = elem.expression();
                if (updateExpr != null) {
                    vex.analyze(updateExpr);
                    Full_column_nameContext fcn = elem.full_column_name();
                    if (fcn != null) {
                        select.addColumnDepcy(fcn);
                    }
                } else {
                    vex.expressionList(elem.expression_list());
                }
            }
        }
    }
}
