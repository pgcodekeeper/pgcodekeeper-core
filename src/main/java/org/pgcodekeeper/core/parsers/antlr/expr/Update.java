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
package org.pgcodekeeper.core.parsers.antlr.expr;

import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.*;
import org.pgcodekeeper.core.parsers.antlr.rulectx.Vex;
import org.pgcodekeeper.core.schema.meta.MetaContainer;
import org.pgcodekeeper.core.utils.ModPair;

import java.util.Collections;
import java.util.List;

/**
 * Parser for UPDATE statements with namespace support.
 */
public class Update extends AbstractExprWithNmspc<Update_stmt_for_psqlContext> {

    protected Update(AbstractExpr parent) {
        super(parent);
    }
    
    /**
     * Creates an Update parser with meta container.
     *
     * @param meta the meta container with schema information
     */
    public Update(MetaContainer meta) {
        super(meta);
    }

    @Override
    public List<ModPair<String, String>> analyze(Update_stmt_for_psqlContext update) {
        // this select is used to collect namespaces for this UPDATE operation
        Select select = new Select(this);
        With_clauseContext with = update.with_clause();
        if (with != null) {
            select.analyzeCte(with);
        }

        select.addNameReference(update.update_table_name, update.alias, null);

        if (update.FROM() != null) {
            select.from(update.from_item());
        }

        for (Update_setContext updateSet : update.update_set()) {
            select.addColumnsDepcies(update.update_table_name, updateSet.column);

            Table_subqueryContext subQuery = updateSet.table_subquery();
            if (subQuery != null) {
                new Select(select).analyze(subQuery.select_stmt());
            } else if (!updateSet.value.isEmpty()) {
                ValueExpr vex = new ValueExpr(select);
                for (VexContext vexCtx : updateSet.value) {
                    vex.analyze(new Vex(vexCtx));
                }
            }
        }

        if (update.WHERE() != null) {
            VexContext vex = update.vex();
            if (vex != null) {
                new ValueExpr(select).analyze(new Vex(vex));
            }
        }

        if (update.RETURNING() != null) {
            return select.sublist(update.select_list().select_sublist(), new ValueExpr(select));
        }

        return Collections.emptyList();
    }
}
