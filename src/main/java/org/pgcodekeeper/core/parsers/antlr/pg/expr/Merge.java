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
package org.pgcodekeeper.core.parsers.antlr.pg.expr;

import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.*;
import org.pgcodekeeper.core.parsers.antlr.pg.rulectx.Vex;
import org.pgcodekeeper.core.utils.ModPair;

import java.util.Collections;
import java.util.List;

/**
 * Parser for MERGE statements with namespace support.
 */
public class Merge extends AbstractExprWithNmspc<Merge_stmt_for_psqlContext>  {

    /**
     * Creates a Merge parser with parent expression context.
     *
     * @param parent the parent expression context
     */
    public Merge(AbstractExpr parent) {
        super(parent);
    }

    @Override
    public List<ModPair<String, String>> analyze(Merge_stmt_for_psqlContext merge) {
        // this select is used to collect namespaces for this MERGE operation
        Select select = new Select(this);

        With_clauseContext with = merge.with_clause();
        if (with != null) {
            select.analyzeCte(with);
        }

        select.addNameReference(merge.merge_table_name, merge.alias, null);
        select.from(List.of(merge.from_item()));

        ValueExpr vexOn = new ValueExpr(select);
        vexOn.analyze(new Vex(merge.vex()));

        for (When_conditionContext whenCondition : merge.when_condition()) {
            Merge_matchedContext match = whenCondition.merge_matched();
            if (match != null) {
                for (Merge_updateContext update : match.merge_update()) {
                    select.addColumnsDepcies(merge.merge_table_name, update.column);
                    for (VexContext vex : update.value) {
                        new ValueExpr(select).analyze(new Vex(vex));
                    }
                }
            } else {
                Merge_not_matchedContext notMatch = whenCondition.merge_not_matched();
                Values_stmtContext selectCtx = notMatch.values_stmt();
                if (selectCtx != null) {
                    ValueExpr vex = new ValueExpr(select);
                    for (Values_valuesContext values : selectCtx.values_values()) {
                        for (VexContext v : values.vex()) {
                            vex.analyze(new Vex(v));
                        }
                    }
                }
            }
        }

        if (merge.RETURNING() != null) {
            return select.sublist(merge.select_list().select_sublist(), new ValueExpr(select));
        }

        return Collections.emptyList();
    }
}
