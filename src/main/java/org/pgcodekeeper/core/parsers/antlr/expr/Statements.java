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

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.*;
import org.pgcodekeeper.core.schema.meta.MetaContainer;
import org.pgcodekeeper.core.utils.ModPair;

import java.util.Collections;
import java.util.List;

/**
 * Abstract base class for statement parsers with namespace support.
 * Handles common data statement parsing functionality.
 *
 * @param <T> the type of parser rule context this statement processor handles
 */
public abstract class Statements<T extends ParserRuleContext> extends AbstractExprWithNmspc<T> {

    protected Statements(AbstractExpr parent) {
        super(parent);
    }

    protected Statements(MetaContainer meta) {
        super(meta);
    }

    protected abstract List<StatementContext> getStatements(T ctx);

    @Override
    public List<ModPair<String, String>> analyze(T ctx) {
        for (StatementContext st : getStatements(ctx)) {
            Data_statementContext data = st.data_statement();
            if (data != null) {
                data(data);
            }
        }

        return Collections.emptyList();
    }

    /**
     * Analyzes and processes a data statement.
     *
     * @param data the data statement context to analyze
     * @return list of modified pairs containing analyzed results
     */
    public List<ModPair<String, String>> data(Data_statementContext data) {
        Select_stmtContext selCtx = data.select_stmt();
        Insert_stmt_for_psqlContext insCtx;
        Update_stmt_for_psqlContext updCtx;
        Merge_stmt_for_psqlContext merCtx;
        Delete_stmt_for_psqlContext delCtx;
        if (selCtx != null) {
            return new Select(this).analyze(selCtx);
        }
        if ((insCtx = data.insert_stmt_for_psql()) != null) {
            return new Insert(this).analyze(insCtx);
        }
        if ((updCtx = data.update_stmt_for_psql()) != null) {
            return new Update(this).analyze(updCtx);
        }
        if ((merCtx = data.merge_stmt_for_psql()) != null) {
            return new Merge(this).analyze(merCtx);
        }
        if ((delCtx = data.delete_stmt_for_psql()) != null) {
            return new Delete(this).analyze(delCtx);
        }
        return Collections.emptyList();
    }
}
