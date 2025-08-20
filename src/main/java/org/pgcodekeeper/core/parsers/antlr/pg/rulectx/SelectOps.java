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
package org.pgcodekeeper.core.parsers.antlr.pg.rulectx;

import org.antlr.v4.runtime.tree.TerminalNode;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.*;

/**
 * Merging wrapper for select_ops/select_ops_no_parens.
 * Provides a unified interface for working with PostgreSQL SELECT operations
 * that may or may not be parenthesized.
 *
 * @author levsha_aa
 */
public class SelectOps {

    private final Select_opsContext ops;
    private final Select_ops_no_parensContext opsNp;
    private final boolean isNp;

    /**
     * Creates a wrapper for parenthesized SELECT operations.
     *
     * @param ops the SELECT operations context with parentheses
     */
    public SelectOps(Select_opsContext ops) {
        this.ops = ops;
        this.opsNp = null;
        this.isNp = false;
    }

    /**
     * Creates a wrapper for non-parenthesized SELECT operations.
     *
     * @param ops the SELECT operations context without parentheses
     */
    public SelectOps(Select_ops_no_parensContext ops) {
        this.opsNp = ops;
        this.ops = null;
        this.isNp = true;
    }

    /**
     * Returns the left parenthesis terminal node if present.
     *
     * @return the left parenthesis node or null if not present
     */
    public TerminalNode leftParen() {
        return isNp ? opsNp.LEFT_PAREN() : ops.LEFT_PAREN();
    }

    /**
     * Returns the right parenthesis terminal node if present.
     *
     * @return the right parenthesis node or null if not present
     */
    public TerminalNode rightParen() {
        return isNp ? opsNp.RIGHT_PAREN() : ops.RIGHT_PAREN();
    }

    /**
     * Returns the SELECT statement context.
     *
     * @return the SELECT statement context or null for non-parenthesized operations
     */
    public Select_stmtContext selectStmt() {
        return isNp ? null : ops.select_stmt();
    }

    /**
     * Returns a SELECT operations wrapper for the specified index.
     *
     * @param i the index of the SELECT operations
     * @return SELECT operations wrapper or null if not found
     */
    public SelectOps selectOps(int i) {
        Select_opsContext ctx = null;
        if (isNp && i == 0) {
            ctx = opsNp.select_ops();
        } else if (!isNp) {
            ctx = ops.select_ops(i);
        }

        return ctx == null ? null : new SelectOps(ctx);
    }

    /**
     * Returns the INTERSECT terminal node if present.
     *
     * @return the INTERSECT node or null if not present
     */
    public TerminalNode intersect() {
        return isNp ? opsNp.INTERSECT() : ops.INTERSECT();
    }

    /**
     * Returns the UNION terminal node if present.
     *
     * @return the UNION node or null if not present
     */
    public TerminalNode union() {
        return isNp ? opsNp.UNION() : ops.UNION();
    }

    /**
     * Returns the EXCEPT terminal node if present.
     *
     * @return the EXCEPT node or null if not present
     */
    public TerminalNode except() {
        return isNp ? opsNp.EXCEPT() : ops.EXCEPT();
    }

    /**
     * Returns the set qualifier context (ALL, DISTINCT).
     *
     * @return the set qualifier context or null if not present
     */
    public Set_qualifierContext setQualifier() {
        return isNp ? opsNp.set_qualifier() : ops.set_qualifier();
    }

    /**
     * Returns the SELECT primary context.
     *
     * @return the SELECT primary context or null if not present
     */
    public Select_primaryContext selectPrimary() {
        return isNp ? opsNp.select_primary() : ops.select_primary();
    }
}
