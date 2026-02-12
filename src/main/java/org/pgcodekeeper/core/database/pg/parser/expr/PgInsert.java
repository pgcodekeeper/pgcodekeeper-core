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
import org.pgcodekeeper.core.utils.ModPair;

/**
 * Parser for INSERT statements with namespace support.
 */
public class PgInsert extends PgAbstractExprWithNmspc<Insert_stmt_for_psqlContext> {

    protected PgInsert(PgAbstractExpr parent) {
        super(parent);
    }

    /**
     * Creates an Insert parser with meta container.
     *
     * @param meta the meta container with schema information
     */
    public PgInsert(IMetaContainer meta) {
        super(meta);
    }

    @Override
    public List<ModPair<String, String>> analyze(Insert_stmt_for_psqlContext insert) {
        // this select is used to collect namespaces for this INSERT operation
        PgSelect select = new PgSelect(this);
        With_clauseContext with = insert.with_clause();
        if (with != null) {
            select.analyzeCte(with);
        }

        var table = select.addNameReference(insert.insert_table_name, null, null);

        Insert_columnsContext columns = insert.insert_columns();
        if (columns != null) {
            select.addColumnsDepcies(insert.insert_table_name, columns.indirection_identifier());
        }

        Select_stmtContext selectCtx = insert.select_stmt();
        if (selectCtx != null) {
            new PgSelect(select).analyze(selectCtx);
        }

        return analyzeReturningSelectList(select, insert.returning_select_list_with_alias(), table);
    }
}
