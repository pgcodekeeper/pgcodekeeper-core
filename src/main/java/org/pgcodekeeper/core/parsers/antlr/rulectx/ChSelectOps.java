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

import org.antlr.v4.runtime.tree.TerminalNode;
import org.pgcodekeeper.core.parsers.antlr.generated.CHParser.Select_opsContext;
import org.pgcodekeeper.core.parsers.antlr.generated.CHParser.Select_ops_no_parensContext;
import org.pgcodekeeper.core.parsers.antlr.generated.CHParser.Select_primaryContext;
import org.pgcodekeeper.core.parsers.antlr.generated.CHParser.Select_stmtContext;

import java.util.List;

/**
 * Merging wrapper for ClickHouse SELECT operations.
 * Provides a unified interface for working with ClickHouse SELECT operations
 * that may or may not be parenthesized.
 */
public class ChSelectOps {

    private final Select_opsContext ops;
    private final Select_ops_no_parensContext opsNp;
    private final boolean isNp;

    /**
     * Creates a wrapper for parenthesized ClickHouse SELECT operations.
     *
     * @param ops the SELECT operations context with parentheses
     */
    public ChSelectOps(Select_opsContext ops) {
        this.ops = ops;
        this.opsNp = null;
        this.isNp = false;
    }

    /**
     * Creates a wrapper for non-parenthesized ClickHouse SELECT operations.
     *
     * @param ops the SELECT operations context without parentheses
     */
    public ChSelectOps(Select_ops_no_parensContext ops) {
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
        return isNp ? null : ops.LPAREN();
    }

    /**
     * Returns the right parenthesis terminal node if present.
     *
     * @return the right parenthesis node or null if not present
     */
    public TerminalNode rightParen() {
        return isNp ? null : ops.RPAREN();
    }

    public Select_stmtContext selectStmt() {
        return isNp ? null : ops.select_stmt();
    }

    public List<Select_opsContext> selectOps() {
        return isNp ? opsNp.select_ops() : ops.select_ops();
    }

    public ChSelectOps selectOps(int i) {
        Select_opsContext ctx = isNp ? opsNp.select_ops(i) : ops.select_ops(i);
        return ctx == null ? null : new ChSelectOps(ctx);
    }

    public TerminalNode union() {
        return isNp ? opsNp.UNION() : ops.UNION();
    }

    public TerminalNode all() {
        return isNp ? opsNp.ALL() : ops.ALL();
    }

    public TerminalNode intersect() {
        return isNp ? opsNp.INTERSECT() : ops.INTERSECT();
    }

    public TerminalNode except() {
        return isNp ? opsNp.EXCEPT() : ops.EXCEPT();
    }

    public TerminalNode distinct() {
        return isNp ? opsNp.DISTINCT() : ops.DISTINCT();
    }

    public Select_primaryContext selectPrimary() {
        return isNp ? opsNp.select_primary() : ops.select_primary();
    }
}
