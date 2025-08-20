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
package org.pgcodekeeper.core.parsers.antlr.ms.rulectx;

import org.antlr.v4.runtime.tree.TerminalNode;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.*;

import java.util.List;

/**
 * Merging wrapper for Microsoft SQL SELECT operations.
 * Provides a unified interface for working with Microsoft SQL SELECT operations
 * that may or may not be parenthesized.
 */
public class MsSelectOps {

    private final Select_opsContext ops;
    private final Select_ops_no_parensContext opsNp;
    private final boolean isNp;

    /**
     * Creates a wrapper for parenthesized Microsoft SQL SELECT operations.
     *
     * @param ops the SELECT operations context with parentheses
     */
    public MsSelectOps(Select_opsContext ops) {
        this.ops = ops;
        this.opsNp = null;
        this.isNp = false;
    }

    /**
     * Creates a wrapper for non-parenthesized Microsoft SQL SELECT operations.
     *
     * @param ops the SELECT operations context without parentheses
     */
    public MsSelectOps(Select_ops_no_parensContext ops) {
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
        return isNp ? null : ops.LR_BRACKET();
    }

    /**
     * Returns the right parenthesis terminal node if present.
     *
     * @return the right parenthesis node or null if not present
     */
    public TerminalNode rightParen() {
        return isNp ? null : ops.RR_BRACKET();
    }

    /**
     * Returns the SELECT statement context.
     *
     * @return the SELECT statement context or null for non-parenthesized operations
     */
    public Select_statementContext selectStmt() {
        return isNp ? null : ops.select_statement();
    }

    public List<Select_opsContext> selectOps() {
        return isNp ? opsNp.select_ops() : ops.select_ops();
    }

    public MsSelectOps selectOps(int i) {
        Select_opsContext ctx = isNp ? opsNp.select_ops(i) : ops.select_ops(i);
        return ctx == null ? null : new MsSelectOps(ctx);
    }

    public TerminalNode intersect() {
        return isNp ? opsNp.INTERSECT() : ops.INTERSECT();
    }

    public TerminalNode union() {
        return isNp ? opsNp.UNION() : ops.UNION();
    }

    public TerminalNode except() {
        return isNp ? opsNp.EXCEPT() : ops.EXCEPT();
    }

    public Set_qualifierContext setQualifier() {
        return isNp ? opsNp.set_qualifier() : ops.set_qualifier();
    }

    public Query_specificationContext querySpecification() {
        return isNp ? opsNp.query_specification() : ops.query_specification();
    }
}
