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
package org.pgcodekeeper.core.parsers.antlr.rulectx;

import org.pgcodekeeper.core.parsers.antlr.generated.TSQLParser.*;

/**
 * Merging wrapper for Microsoft SQL SELECT statements.
 * Provides a unified interface for working with both parenthesized and non-parenthesized
 * SELECT statements in Microsoft SQL syntax.
 */
public class MsSelectStmt {

    private final Select_statementContext select;
    private final Select_stmt_no_parensContext selectNp;
    private final boolean isNp;

    /**
     * Creates a wrapper for a Microsoft SQL SELECT statement.
     *
     * @param select the SELECT statement context
     */
    public MsSelectStmt(Select_statementContext select) {
        this.select = select;
        this.selectNp = null;
        this.isNp = false;
    }

    /**
     * Creates a wrapper for a Microsoft SQL SELECT statement without parentheses.
     *
     * @param select the SELECT statement context without parentheses
     */
    public MsSelectStmt(Select_stmt_no_parensContext select) {
        this.selectNp = select;
        this.select = null;
        this.isNp = true;
    }

    /**
     * Returns the WITH expression context if present.
     *
     * @return the WITH expression context or null if not present
     */
    public With_expressionContext withExpression() {
        return isNp ? selectNp.with_expression() : select.with_expression();
    }

    /**
     * Returns the Microsoft SQL SELECT operations wrapper.
     *
     * @return wrapper for the SELECT operations part of the statement
     */
    public MsSelectOps selectOps() {
        // no null check since select_ops is mandatory in select_stmt
        return isNp ? new MsSelectOps(selectNp.select_ops_no_parens())
                : new MsSelectOps(select.select_ops());
    }

    /**
     * Returns the OPTION clause context if present.
     *
     * @return the OPTION clause context or null if not present
     */
    public Option_clauseContext option() {
        return isNp ? selectNp.option_clause() : select.option_clause();
    }

    /**
     * Returns the FOR clause context if present.
     *
     * @return the FOR clause context or null if not present
     */
    public For_clauseContext forClause() {
        return isNp ? selectNp.for_clause() : select.for_clause();
    }
}
