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
package org.pgcodekeeper.core.parsers.antlr.pg.expr;

import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.Function_bodyContext;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.Function_returnContext;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.StatementContext;
import org.pgcodekeeper.core.parsers.antlr.pg.rulectx.Vex;
import org.pgcodekeeper.core.schema.meta.MetaContainer;
import org.pgcodekeeper.core.utils.ModPair;

import java.util.Collections;
import java.util.List;

/**
 * Parser for SQL function body statements with namespace support.
 */
public class SqlFunctionBody extends Statements<Function_bodyContext> {

    /**
     * Creates a SqlFunctionBody parser with meta container.
     *
     * @param meta the meta container with schema information
     */
    public SqlFunctionBody(MetaContainer meta) {
        super(meta);
    }

    @Override
    protected List<StatementContext> getStatements(Function_bodyContext ctx) {
        return ctx.statement();
    }

    /**
     * Analyzes a function body context and returns empty result list.
     *
     * @param ctx the function body context to analyze
     * @return empty list as function body doesn't produce result types
     */
    @Override
    public List<ModPair<String, String>> analyze(Function_bodyContext ctx) {
        super.analyze(ctx);
        for (Function_returnContext ret : ctx.function_return()) {
            new ValueExpr(this).analyze(new Vex(ret.vex()));
        }

        return Collections.emptyList();
    }
}
