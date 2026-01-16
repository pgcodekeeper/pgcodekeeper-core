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
package org.pgcodekeeper.core.database.pg.parser.expr;

import java.util.*;

import org.pgcodekeeper.core.database.base.schema.meta.MetaContainer;
import org.pgcodekeeper.core.database.pg.parser.generated.SQLParser.*;
import org.pgcodekeeper.core.database.pg.parser.rulectx.PgVex;
import org.pgcodekeeper.core.utils.ModPair;

/**
 * Parser for SQL function body statements with namespace support.
 */
public class PgSqlFunctionBody extends PgAbstractStatements<Function_bodyContext> {

    /**
     * Creates a SqlFunctionBody parser with meta container.
     *
     * @param meta the meta container with schema information
     */
    public PgSqlFunctionBody(MetaContainer meta) {
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
            new PgValueExpr(this).analyze(new PgVex(ret.vex()));
        }

        return Collections.emptyList();
    }
}
