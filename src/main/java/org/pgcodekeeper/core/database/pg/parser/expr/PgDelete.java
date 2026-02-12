/*******************************************************************************
 * Copyright 2017-2026 TAXTELECOM, LLC
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
package org.pgcodekeeper.core.database.pg.parser.expr;

import java.util.List;

import org.pgcodekeeper.core.database.api.schema.meta.IMetaContainer;
import org.pgcodekeeper.core.database.pg.parser.generated.SQLParser.*;
import org.pgcodekeeper.core.database.pg.parser.rulectx.PgVex;
import org.pgcodekeeper.core.utils.ModPair;

/**
 * Parser for DELETE statements with namespace support.
 */
public class PgDelete extends PgAbstractExprWithNmspc<Delete_stmt_for_psqlContext> {

    protected PgDelete(PgAbstractExpr parent) {
        super(parent);
    }

    /**
     * Creates a Delete parser with meta container.
     *
     * @param meta the meta container with schema information
     */
    public PgDelete(IMetaContainer meta) {
        super(meta);
    }

    @Override
    public List<ModPair<String, String>> analyze(Delete_stmt_for_psqlContext delete) {
        // this select is used to collect namespaces for this DELETE operation
        PgSelect select = new PgSelect(this);
        With_clauseContext with = delete.with_clause();
        if (with != null) {
            select.analyzeCte(with);
        }

        var table = select.addNameReference(delete.delete_table_name, delete.alias, null);
        if (delete.USING() != null) {
            select.from(delete.from_item());
        }

        if (delete.WHERE() != null) {
            VexContext vex = delete.vex();
            if (vex != null) {
                new PgValueExpr(select).analyze(new PgVex(vex));
            }
        }

        return analyzeReturningSelectList(select, delete.returning_select_list_with_alias(), table);
    }
}
