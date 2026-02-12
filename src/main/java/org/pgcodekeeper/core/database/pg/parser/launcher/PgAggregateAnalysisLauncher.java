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
package org.pgcodekeeper.core.database.pg.parser.launcher;

import java.util.*;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.api.schema.meta.IMetaContainer;
import org.pgcodekeeper.core.database.base.parser.launcher.AbstractAnalysisLauncher;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;

/**
 * Specialized launcher for analyzing aggregate function dependencies.
 * Handles type propagation between aggregate functions and their arguments.
 */
public class PgAggregateAnalysisLauncher extends AbstractAnalysisLauncher {

    private final GenericColumn function;

    /**
     * Creates an analyzer for aggregate function statements.
     *
     * @param stmt     the aggregate function statement
     * @param function the function being analyzed
     * @param location the source location identifier
     */
    public PgAggregateAnalysisLauncher(AbstractStatement stmt, GenericColumn function,
                                     String location) {
        super(stmt, null, location);
        this.function = function;
    }

    @Override
    protected Set<ObjectLocation> analyze(ParserRuleContext ctx,
                                          IMetaContainer meta) {

        IFunction func = meta.findFunction(function.schema(), function.table());
        IFunction aggr = meta.findFunction(getSchemaName(), stmt.getName());

        if (aggr != null && func != null) {
            aggr.setReturns(func.getReturns());
        }

        return Collections.emptySet();
    }
}
