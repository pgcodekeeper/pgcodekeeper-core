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

import org.pgcodekeeper.core.parsers.antlr.ch.generated.CHParser.ExprContext;
import org.pgcodekeeper.core.database.base.schema.meta.MetaContainer;

import java.util.Collections;
import java.util.List;

/**
 * Concrete implementation of ChAbstractExprWithNmspc for analyzing ClickHouse SQL expressions.
 * Provides specific handling of expression contexts with namespace support.
 */
public final class ChExprWithNmspc extends ChAbstractExprWithNmspc<ExprContext> {

    private final ChValueExpr expr;

    /**
     * Constructs a new expression analyzer with schema and metadata container.
     *
     * @param schema the database schema name
     * @param meta   the metadata container for database objects
     */
    public ChExprWithNmspc(String schema, MetaContainer meta) {
        super(schema, meta);
        expr = new ChValueExpr(this);
    }

    /**
     * Analyzes the given expression context and returns an empty list.
     *
     * @param ruleCtx the ANTLR expression context to analyze
     * @return empty list
     */
    @Override
    public List<String> analyze(ExprContext ruleCtx) {
        expr.analyze(ruleCtx);
        return Collections.emptyList();
    }
}
