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
package org.pgcodekeeper.core.parsers.antlr.ch.expr;

import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.parsers.antlr.ch.generated.CHParser.*;
import org.pgcodekeeper.core.parsers.antlr.ch.rulectx.ChSelectOps;
import org.pgcodekeeper.core.parsers.antlr.ch.rulectx.ChSelectStmt;
import org.pgcodekeeper.core.database.base.schema.meta.MetaContainer;

import java.util.Collections;
import java.util.List;

/**
 * Handles parsing and analysis of ClickHouse SELECT statements.
 * Manages query structure including FROM, WHERE, GROUP BY clauses and more.
 */
public final class ChSelect extends ChAbstractExprWithNmspc<Select_stmtContext> {

    private final ChValueExpr vex = new ChValueExpr(this);

    /**
     * Creates a new SELECT analyzer with parent context.
     *
     * @param parent the parent expression analyzer
     */
    ChSelect(ChAbstractExpr parent) {
        super(parent, true);
    }

    /**
     * Creates a new SELECT analyzer with schema and metadata.
     *
     * @param schema the database schema name
     * @param meta   the metadata container
     */
    public ChSelect(String schema, MetaContainer meta) {
        super(schema, meta);
    }

    /**
     * Analyzes a SELECT statement context.
     *
     * @param ruleCtx the ANTLR parse tree context
     * @return empty list (analysis results are stored internally)
     */
    @Override
    public List<String> analyze(Select_stmtContext ruleCtx) {
        return analyze(new ChSelectStmt(ruleCtx));
    }

    /**
     * Analyzes a SELECT statement without parentheses context.
     *
     * @param ruleCtx the ANTLR parse tree context
     * @return empty list (analysis results are stored internally)
     */
    public List<String> analyze(Select_stmt_no_parensContext ruleCtx) {
        return analyze(new ChSelectStmt(ruleCtx));
    }

    /**
     * Analyzes a pre-parsed SELECT statement structure.
     *
     * @param select the pre-parsed SELECT statement wrapper
     * @return empty list (analysis results are stored internally)
     */
    public List<String> analyze(ChSelectStmt select) {
        return selectOps(select.selectOps());
    }

    private List<String> selectOps(ChSelectOps selectOps) {
        Select_stmtContext selectStmt = selectOps.selectStmt();
        if (selectStmt != null) {
            return analyze(selectStmt);
        }

        if (selectOps.intersect() != null || selectOps.union() != null || selectOps.except() != null) {
            // analyze each in a separate scope
            // use column names from the first one
            var ret = new ChSelect(this).selectOps(selectOps.selectOps(0));
            new ChSelect(this).selectOps(selectOps.selectOps(1));
            return ret;
        }

        Select_primaryContext query = selectOps.selectPrimary();
        if (query != null) {
            return select(query);
        }

        log(Messages.ChSelect_log_not_alter_selectops);
        return Collections.emptyList();
    }

    private List<String> select(Select_primaryContext query) {
        analyzeCte(query.with_clause());

        var from = query.from_clause();
        if (from != null) {
            from.from_item().forEach(this::from);
        }

        vex.analyzeExprs(query.select_list().expr());

        var arrayJoin = query.array_join_clause();
        if (arrayJoin != null) {
            vex.analyzeExprs(arrayJoin.expr_list().expr());
        }

        var window = query.window_clause();
        if (window != null) {
            vex.window(window.window_expr());
        }

        var preWhere = query.prewhere_clause();
        if (preWhere != null) {
            vex.analyze(preWhere.expr());
        }

        var where = query.where_clause();
        if (where != null) {
            vex.analyze(where.expr());
        }

        var groupBy = query.group_by_clause();
        if (groupBy != null) {
            groupBy(groupBy.grouping_element_list());
        }

        var having = query.having_clause();
        if (having != null) {
            vex.analyze(having.expr());
        }

        vex.orderBy(query.order_by_clause());

        var limitBy = query.limit_by_clause();
        if (limitBy != null) {
            vex.analyzeExprs(limitBy.expr_list().expr());
        }

        return Collections.emptyList();
    }

    private void from(From_itemContext fromItem) {
        fromItem.from_item().forEach(this::from);
        if (fromItem.ON() != null || fromItem.USING() != null) {
            vex.analyzeExprs(fromItem.expr_list().expr());
        }

        var primary = fromItem.from_primary();
        if (primary == null) {
            return;
        }

        var alias = fromItem.alias_clause();
        if (primary.qualified_name() != null) {
            addReferenceInRootParent(primary.qualified_name(), alias, true);
        }
        if (alias != null) {
            addReference(getAliasCtx(alias).getText());
        }
        primary(primary);
    }

    private void groupBy(Grouping_element_listContext groupByList) {
        if (groupByList == null) {
            return;
        }

        for (var groupEl : groupByList.grouping_element()) {
            groupBy(groupEl.grouping_element_list());
            var expr = groupEl.expr();
            if (expr != null) {
                vex.analyze(expr);
            }
        }
    }

    private void primary(From_primaryContext primary) {
        Select_stmt_no_parensContext selectOps = primary.select_stmt_no_parens();
        if (selectOps != null) {
            new ChSelect(this).analyze(selectOps);
            return;
        }
        vex.functionCall(primary.function_call());
    }
}