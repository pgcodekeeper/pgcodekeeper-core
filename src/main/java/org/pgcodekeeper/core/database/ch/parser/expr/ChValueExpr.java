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
package org.pgcodekeeper.core.database.ch.parser.expr;

import java.util.*;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.api.schema.meta.IMetaContainer;
import org.pgcodekeeper.core.database.base.parser.QNameParser;
import org.pgcodekeeper.core.database.ch.parser.generated.CHParser.*;
import org.pgcodekeeper.core.utils.Pair;

/**
 * Handles parsing and analysis of ClickHouse value expressions in SQL queries.
 * Processes various expression types including function calls, column references and subqueries.
 */
public final class ChValueExpr extends ChAbstractExpr {

    ChValueExpr(ChAbstractExpr parent) {
        super(parent);
    }

    /**
     * Creates a value expression analyzer with metadata container.
     *
     * @param meta the metadata container for database objects
     */
    public ChValueExpr(IMetaContainer meta) {
        super(null, meta);
    }

    /**
     * Analyzes an expression context.
     *
     * @param expr the ANTLR expression context to analyze
     */
    public void analyze(ExprContext expr) {
        var aliasExpr = expr.alias_expr();
        analyzeExprs(expr.expr());

        primary(expr.expr_primary(), aliasExpr);

        var likeExpr = expr.like_expr();
        if (likeExpr != null) {
            analyze(likeExpr.expr());
        }

        selectMode(expr.select_mode());
    }

    private void selectMode(Select_modeContext selectMode) {
        if (selectMode == null) {
            return;
        }

        if (selectMode.APPLY() != null) {
            functionCall(selectMode.function_call());
            var lambda = selectMode.lambda_expr();
            if (lambda != null) {
                analyze(lambda.expr());
            }
            return;
        }

        if (selectMode.EXCEPT() != null) {
            analyzeExprs(selectMode.expr_list());
            addDynamicColumnDepcies(selectMode.literal(), false);
            return;
        }

        if (selectMode.REPLACE() != null) {
            var exprList = selectMode.expr_list_with_alias().expr();
            analyzeExprs(exprList);
        }
    }

    private void primary(Expr_primaryContext exprPrimary, Alias_exprContext aliasExpr) {
        if (exprPrimary == null) {
            return;
        }

        var fc = exprPrimary.function_call();
        if (fc != null) {
            functionCall(fc);
            return;
        }

        var selectCtx = exprPrimary.select_stmt_no_parens();
        if (selectCtx != null) {
            new ChSelect(this).analyze(selectCtx);
            return;
        }

        var stmtQualNameCtx = exprPrimary.qualified_name();
        if (stmtQualNameCtx != null) {
            if (aliasExpr != null) {
                addReferenceInRootParent(stmtQualNameCtx, aliasExpr.alias_clause(), false);
            }
            qualifiedName(stmtQualNameCtx);
            return;
        }

        analyzeExprs(exprPrimary.expr_list());
        addDynamicColumnDepcies(exprPrimary.literal(), true);
    }

    private void qualifiedName(Qualified_nameContext qualNameCtx) {
        var ids = qualNameCtx.identifier();
        if (ids.size() == 1) {
            var relCol = findColumn(qualNameCtx.getText());
            if (relCol != null) {
                addColumnDepcy(qualNameCtx, relCol);
                return;
            }
        }

        var columnName = QNameParser.getFirstName(ids);
        var tableName = QNameParser.getSecondName(ids);
        var ref = findReference(null, tableName, null);
        if (ref == null) {
            // if we don't found reference by alias try to find table in metadata where tableName will be schemaName
            // and columnName will be relationName
            if (findRelation(tableName, columnName) != null) {
                addObjectDepcy(qualNameCtx);
            }
            return;
        }

        ObjectReference parent = ref.getValue();
        if (parent == null) {
            return;
        }

        var schemaName = QNameParser.getThirdName(ids);
        if (schemaName != null) {
            addDependency(new ObjectReference(schemaName, DbObjType.SCHEMA), QNameParser.getThirdNameCtx(ids));
        }

        var tableCtx = QNameParser.getSecondNameCtx(ids);
        var tName = parent.table();
        if (Objects.equals(tableName, tName)) {
            addDependency(parent, tableCtx);
        }

        var column = new ObjectReference(parent.schema(), tName, columnName, DbObjType.COLUMN);

        addDependency(column, QNameParser.getFirstNameCtx(ids));
        addReference(parent, tableCtx);
    }

    void functionCall(Function_callContext functionCall) {
        if (functionCall == null) {
            return;
        }
        if (functionCall.COLUMNS() != null) {
            addDynamicColumnDepcies(functionCall.column, true);
            return;
        }

        analyzeExprs(functionCall.expr());
        analyzeExprs(functionCall.expr_list());

        var windowExpr = functionCall.window_expr();
        if (windowExpr != null) {
            window(windowExpr);
            return;
        }

        var funcName = functionCall.name;
        if (funcName == null) {
            return;
        }

        var argList = functionCall.arg_list();
        if (argList == null) {
            return;
        }

        List<ExprContext> exprs = argList.arg_expr().stream()
                .map(Arg_exprContext::expr)
                .filter(Objects::nonNull)
                .toList();
        analyzeExprs(exprs);
        ObjectReference depcy = new ObjectReference(funcName.getText(), DbObjType.FUNCTION);
        addDependency(depcy, funcName);
    }

    void window(Window_exprContext windowExpr) {
        analyzeExprs(windowExpr.expr_list());
        orderBy(windowExpr.order_by_clause());
    }

    void orderBy(Order_by_clauseContext orderBy) {
        if (orderBy == null) {
            return;
        }

        var orderList = orderBy.order_expr_list();
        if (orderList == null) {
            return;
        }

        for (var orderExprCtx : orderList.order_expr()) {
            analyze(orderExprCtx.expr());

            var withFillCtx = orderExprCtx.with_fill();
            if (withFillCtx != null) {
                analyzeExprs(withFillCtx.expr());
                analyzeExprs(withFillCtx.expr_list());
            }
        }
    }

    private void analyzeExprs(Expr_listContext exprList) {
        if (exprList != null) {
            analyzeExprs(exprList.expr());
        }
    }

    void analyzeExprs(List<ExprContext> exprs) {
        if (exprs != null) {
            exprs.forEach(this::analyze);
        }
    }

    private void addColumnDepcy(Qualified_nameContext qualNameCtx,
                                Pair<IRelation, Pair<String, String>> relCol) {
        IRelation rel = relCol.getFirst();
        Pair<String, String> col = relCol.getSecond();
        var column = new ObjectReference(rel.getSchemaName(), rel.getName(), col.getFirst(), DbObjType.COLUMN);
        addDependency(column, qualNameCtx);
    }

    private void addDynamicColumnDepcies(LiteralContext lit, boolean include) {
        if (lit == null) {
            return;
        }

        var rawNamePart = lit.getText();
        var contains = rawNamePart.startsWith("'");
        var namePart = rawNamePart.replace("'", "");
        // TODO add when we will understood how processing in ClickHouse this cases when argument is array
        if (namePart.startsWith("[") && namePart.endsWith("]")) {
            return;
        }

        List<ObjectReference> tempDependencies = new ArrayList<>();
        for (var dependency : getDependencies()) {
            var obj = dependency.getObjectReference();
            if (obj.type() != DbObjType.TABLE) {
                continue;
            }
            var rel = findRelation(obj.schema(), obj.table());
            if (rel != null) {
                rel.getRelationColumns()
                .filter(e -> isNeedColumn(e.getFirst(), namePart, include, contains))
                .forEach(e -> tempDependencies.add(
                        new ObjectReference(rel.getSchemaName(), rel.getName(), e.getFirst(), DbObjType.COLUMN)));
            }
        }
        tempDependencies.forEach(e -> addDependency(e, lit));
    }

    /**
     * This method need for understanding from which columns we have depence when user use dynamic column selection
     *
     * @param name     - name of column
     * @param namePart - condition for filter
     * @param include  - true/false include/exclude
     * @param contains - true/false check that name contains/equals namePart
     * @return boolean - if true we will be added this column at the depcies, and if false exclude
     */
    private boolean isNeedColumn(String name, String namePart, boolean include, boolean contains) {
        if (contains) {
            var cont = name.contains(namePart);
            return include == cont;
        }
        var eq = name.equals(namePart);
        return include == eq;
    }
}