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
package org.pgcodekeeper.core.loader;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * SQL query builder for constructing SELECT statements with support for CTEs, joins, WHERE clauses, and GROUP BY.
 * Provides a fluent interface for building complex SQL queries programmatically.
 */
public final class QueryBuilder {

    private final Map<String, String> withs = new LinkedHashMap<>();
    private final List<String> columns = new ArrayList<>();
    private String from;
    private final List<String> joins = new ArrayList<>();
    private final List<String> wheres = new ArrayList<>();
    private final List<String> groups = new ArrayList<>();
    private final List<String> orders = new ArrayList<>();
    private String postAction;

    /**
     * Adds a column to the SELECT clause.
     *
     * @param column the column expression to add
     * @return this builder for method chaining
     */
    public QueryBuilder column(String column) {
        columns.add(column);
        return this;
    }

    /**
     * Adds a column with prefix and postfix using a subquery.
     *
     * @param prefix  text to prepend before the subquery
     * @param column  the subquery builder for the column
     * @param postfix text to append after the subquery
     * @return this builder for method chaining
     */
    public QueryBuilder column(String prefix, QueryBuilder column, String postfix) {
        StringBuilder sb = new StringBuilder();
        if (!prefix.isEmpty()) {
            sb.append(prefix).append(" ");
        }
        appendChild(sb, column, 4);
        sb.append(" ").append(postfix);
        columns.add(sb.toString());
        return this;
    }

    /**
     * Adds a JOIN clause to the query.
     *
     * @param join the JOIN clause to add
     * @return this builder for method chaining
     */
    public QueryBuilder join(String join) {
        joins.add(join);
        return this;
    }

    /**
     * Adds a JOIN clause with prefix and postfix using a subquery.
     *
     * @param prefix  text to prepend before the subquery
     * @param join    the subquery builder for the join
     * @param postfix text to append after the subquery
     * @return this builder for method chaining
     */
    public QueryBuilder join(String prefix, QueryBuilder join, String postfix) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append(" ");
        appendChild(sb, join);
        sb.append(" ").append(postfix);
        joins.add(sb.toString());
        return this;
    }

    /**
     * Sets the FROM clause of the query.
     *
     * @param from the FROM clause
     * @return this builder for method chaining
     */
    public QueryBuilder from(String from) {
        this.from = from;
        return this;
    }

    /**
     * Sets the FROM clause using a subquery with postfix.
     *
     * @param from    the subquery builder for the FROM clause
     * @param postfix text to append after the subquery
     * @return this builder for method chaining
     */
    public QueryBuilder from(QueryBuilder from, String postfix) {
        StringBuilder sb = new StringBuilder();
        appendChild(sb, from);
        sb.append(" ").append(postfix);
        this.from = sb.toString();
        return this;
    }

    /**
     * Adds a WHERE condition to the query.
     *
     * @param where the WHERE condition to add
     * @return this builder for method chaining
     */
    public QueryBuilder where(String where) {
        wheres.add(where);
        return this;
    }

    /**
     * Adds a WHERE condition with prefix using a subquery.
     *
     * @param prefix text to prepend before the subquery
     * @param where  the subquery builder for the WHERE condition
     * @return this builder for method chaining
     */
    public QueryBuilder where(String prefix, QueryBuilder where) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append(" ");
        appendChild(sb, where);
        wheres.add(sb.toString());
        return this;
    }

    /**
     * Sets a post-action clause (e.g., ORDER BY, LIMIT) to append after the main query.
     *
     * @param action the post-action clause
     * @return this builder for method chaining
     */
    public QueryBuilder postAction(String action) {
        postAction = action;
        return this;
    }

    /**
     * Adds a Common Table Expression (CTE) to the WITH clause.
     *
     * @param alias the alias name for the CTE
     * @param cte   the CTE query string
     * @return this builder for method chaining
     */
    public QueryBuilder with(String alias, String cte) {
        withs.put(alias, "(" + cte + ")");
        return this;
    }

    /**
     * Adds a Common Table Expression (CTE) using a subquery builder.
     *
     * @param alias the alias name for the CTE
     * @param cte   the subquery builder for the CTE
     * @return this builder for method chaining
     */
    public QueryBuilder with(String alias, QueryBuilder cte) {
        StringBuilder sb = new StringBuilder();
        appendChild(sb, cte);
        withs.put(alias, sb.toString());
        return this;
    }

    /**
     * Adds a GROUP BY expression to the query.
     *
     * @param group the GROUP BY expression
     * @return this builder for method chaining
     */
    public QueryBuilder groupBy(String group) {
        groups.add(group);
        return this;
    }

    /**
     * Adds a ORDER BY expression to the query.
     *
     * @param order the ORDER BY expression
     * @return this builder for method chaining
     */
    public QueryBuilder orderBy(String order) {
        orders.add(order);
        return this;
    }

    /**
     * Builds and returns the complete SQL query string.
     *
     * @return the constructed SQL query
     */
    public String build() {
        StringBuilder sb = new StringBuilder();
        if (!withs.isEmpty()) {
            sb.append("WITH ");
            for (Entry<String, String> with : withs.entrySet()) {
                sb.append(with.getKey()).append(" AS ").append(with.getValue()).append(",\n");
            }
            sb.setLength(sb.length() - 2);
            sb.append("\n");
        }

        sb.append("SELECT\n  ");
        sb.append(String.join(",\n  ", columns));

        if (from != null) {
            sb.append("\nFROM ").append(from);
        }

        for (String join : joins) {
            sb.append("\n").append(join);
        }

        appendList(wheres, "\nWHERE ", "\n  AND ", sb);
        appendList(groups, "\nGROUP BY ", ", ", sb);
        appendList(orders, "\nORDER BY ", ", ", sb);

        if (postAction != null) {
            sb.append("\n").append(postAction);
        }

        return sb.toString();
    }

    private void appendList(List<String> list, String prefix, String delimiter, StringBuilder sb) {
        if (!list.isEmpty()) {
            sb.append(prefix).append(String.join(delimiter, list));
        }
    }

    private void appendChild(StringBuilder sb, QueryBuilder childBuilder) {
        appendChild(sb, childBuilder, 2);
    }

    private void appendChild(StringBuilder sb, QueryBuilder childBuilder, int indent) {
        sb.append("(\n").append(childBuilder.build().indent(indent)).append(" ".repeat(indent - 2)).append(")");
    }

    /**
     * Creates a deep copy of this query builder.
     *
     * @return a new QueryBuilder instance with the same configuration
     */
    public QueryBuilder copy() {
        QueryBuilder copy = new QueryBuilder();
        copy.withs.putAll(withs);
        copy.columns.addAll(columns);
        copy.from = from;
        copy.joins.addAll(joins);
        copy.wheres.addAll(wheres);
        copy.groups.addAll(groups);
        copy.postAction = postAction;

        return copy;
    }
}
