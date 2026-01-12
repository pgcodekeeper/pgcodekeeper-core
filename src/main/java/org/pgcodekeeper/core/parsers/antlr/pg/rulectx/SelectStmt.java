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
package org.pgcodekeeper.core.parsers.antlr.pg.rulectx;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.After_opsContext;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.Select_stmtContext;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.Select_stmt_no_parensContext;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.With_clauseContext;

import java.util.List;

/**
 * Merging wrapper for select_stmt/select_stmt_no_parens.
 * Provides a unified interface for working with both parenthesized and non-parenthesized
 * SELECT statements in PostgreSQL syntax.
 *
 * @author levsha_aa
 */
public class SelectStmt {

    private final Select_stmtContext select;
    private final Select_stmt_no_parensContext selectNp;
    private final boolean isNp;

    /**
     * Creates a wrapper for a parenthesized SELECT statement.
     *
     * @param select the SELECT statement context with parentheses
     */
    public SelectStmt(Select_stmtContext select) {
        this.select = select;
        this.selectNp = null;
        this.isNp = false;
    }

    /**
     * Creates a wrapper for a non-parenthesized SELECT statement.
     *
     * @param select the SELECT statement context without parentheses
     */
    public SelectStmt(Select_stmt_no_parensContext select) {
        this.selectNp = select;
        this.select = null;
        this.isNp = true;
    }

    /**
     * Returns the underlying parser rule context.
     *
     * @return the parser rule context for this SELECT statement
     */
    public ParserRuleContext getCtx() {
        return isNp ? selectNp : select;
    }

    /**
     * Returns the WITH clause context if present.
     *
     * @return the WITH clause context or null if not present
     */
    public With_clauseContext withClause() {
        return isNp ? selectNp.with_clause() : select.with_clause();
    }

    /**
     * Returns the SELECT operations wrapper.
     *
     * @return wrapper for the SELECT operations part of the statement
     */
    public SelectOps selectOps() {
        // no null check since select_ops is mandatory in select_stmt
        return isNp ? new SelectOps(selectNp.select_ops_no_parens())
                : new SelectOps(select.select_ops());
    }

    /**
     * Returns the list of operations that come after the main SELECT.
     *
     * @return list of after operations contexts (ORDER BY, LIMIT, etc.)
     */
    public List<After_opsContext> afterOps() {
        return isNp ? selectNp.after_ops() : select.after_ops();
    }
}
