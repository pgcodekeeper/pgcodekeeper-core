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

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Merging wrapper for vex/vex_b.
 * Provides a unified interface for working with PostgreSQL value expressions
 * that can be either regular expressions (vex) or boolean expressions (vex_b).
 *
 * @author levsha_aa
 */
public class Vex {

    private final VexContext vex;
    private final Vex_bContext vexB;
    private final boolean isB;

    /**
     * Creates a wrapper for a regular value expression (vex).
     *
     * @param vex the value expression context
     */
    public Vex(VexContext vex) {
        this.vex = vex;
        this.vexB = null;
        this.isB = false;
    }

    /**
     * Creates a wrapper for a boolean value expression (vex_b).
     *
     * @param vex the boolean value expression context
     */
    public Vex(Vex_bContext vex) {
        this.vex = null;
        this.vexB = vex;
        this.isB = true;
    }

    /**
     * Returns a list of child value expression wrappers.
     *
     * @return list of child Vex wrappers found in this expression
     */
    public List<Vex> vex() {
        List<ParseTree> children = (isB ? vexB : vex).children;
        if (children == null || children.isEmpty()) {
            return Collections.emptyList();
        }
        List<Vex> vex = new ArrayList<>();
        for (ParseTree node : children) {
            if (node instanceof VexContext vexCtx) {
                vex.add(new Vex(vexCtx));
            } else if (node instanceof Vex_bContext vexbCtx) {
                vex.add(new Vex(vexbCtx));
            }
        }
        return vex;
    }

    /**
     * Returns the underlying parser rule context.
     *
     * @return the parser rule context for this expression
     */
    public ParserRuleContext getVexCtx() {
        return isB ? vexB : vex;
    }

    public TerminalNode castExpression() {
        return isB ? vexB.CAST_EXPRESSION() : vex.CAST_EXPRESSION();
    }

    public Data_typeContext dataType() {
        return isB ? vexB.data_type() : vex.data_type();
    }

    public Collate_identifierContext collateIdentifier() {
        return isB ? null : vex.collate_identifier();
    }

    public TerminalNode leftParen() {
        return isB ? vexB.LEFT_PAREN() : vex.LEFT_PAREN();
    }

    public TerminalNode rightParen() {
        return isB ? vexB.RIGHT_PAREN() : vex.RIGHT_PAREN();
    }

    public Indirection_listContext indirectionList() {
        return isB ? vexB.indirection_list() : vex.indirection_list();
    }

    public TerminalNode in() {
        return isB ? null : vex.IN();
    }

    public Select_stmt_no_parensContext selectStmt() {
        return isB ? null : vex.select_stmt_no_parens();
    }

    public Type_listContext typeList() {
        return isB ? vexB.type_list() : vex.type_list();
    }

    public Value_expression_primaryContext primary() {
        return isB ? vexB.value_expression_primary() : vex.value_expression_primary();
    }

    public TerminalNode plus() {
        return isB ? vexB.PLUS() : vex.PLUS();
    }

    public TerminalNode minus() {
        return isB ? vexB.MINUS() : vex.MINUS();
    }

    public TerminalNode timeZone() {
        return isB ? null : vex.ZONE();
    }

    public TerminalNode exp() {
        return isB ? vexB.EXP() : vex.EXP();
    }

    public TerminalNode multiply() {
        return isB ? vexB.MULTIPLY() : vex.MULTIPLY();
    }

    public TerminalNode divide() {
        return isB ? vexB.DIVIDE() : vex.DIVIDE();
    }

    public TerminalNode modular() {
        return isB ? vexB.MODULAR() : vex.MODULAR();
    }

    public OpContext op() {
        return isB ? vexB.op() : vex.op();
    }

    public TerminalNode between() {
        return isB ? null : vex.BETWEEN();
    }

    public TerminalNode like() {
        return isB ? null : vex.LIKE();
    }

    public TerminalNode ilike() {
        return isB ? null : vex.ILIKE();
    }

    public TerminalNode similar() {
        return isB ? null : vex.SIMILAR();
    }

    public TerminalNode lth() {
        return isB ? vexB.LTH() : vex.LTH();
    }

    public TerminalNode gth() {
        return isB ? vexB.GTH() : vex.GTH();
    }

    public TerminalNode leq() {
        return isB ? vexB.LEQ() : vex.LEQ();
    }

    public TerminalNode geq() {
        return isB ? vexB.GEQ() : vex.GEQ();
    }

    public TerminalNode eq() {
        return isB ? vexB.EQUAL() : vex.EQUAL();
    }

    public TerminalNode notEqual() {
        return isB ? vexB.NOT_EQUAL() : vex.NOT_EQUAL();
    }

    public TerminalNode is() {
        return isB ? vexB.IS() : vex.IS();
    }

    public Truth_valueContext truthValue() {
        return isB ? null : vex.truth_value();
    }

    public TerminalNode nullValue() {
        return isB ? null : vex.NULL();
    }

    public TerminalNode distinct() {
        return isB ? vexB.DISTINCT() : vex.DISTINCT();
    }

    public TerminalNode document() {
        return isB ? vexB.DOCUMENT() : vex.DOCUMENT();
    }

    public TerminalNode isNull() {
        return isB ? null : vex.ISNULL();
    }

    public TerminalNode notNull() {
        return isB ? null : vex.NOTNULL();
    }

    public TerminalNode not() {
        return isB ? null : vex.NOT();
    }

    public TerminalNode and() {
        return isB ? null : vex.AND();
    }

    public TerminalNode or() {
        return isB ? null : vex.OR();
    }
}