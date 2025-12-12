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
package org.pgcodekeeper.core.parsers.antlr.pg.launcher;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.Index_columnContext;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.Index_restContext;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.Storage_parameter_optionContext;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.Storage_parametersContext;
import org.pgcodekeeper.core.database.api.schema.ObjectLocation;
import org.pgcodekeeper.core.database.base.schema.meta.MetaContainer;
import org.pgcodekeeper.core.database.pg.schema.PgIndex;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Launcher for analyzing PostgreSQL index definitions.
 * Extracts dependencies from index columns, storage parameters and WHERE clauses.
 */
public class IndexAnalysisLauncher extends AbstractPgAnalysisLauncher {

    /**
     * Creates an index analyzer.
     *
     * @param stmt     the index statement to analyze
     * @param ctx      the index definition context
     * @param location the source location identifier
     */

    public IndexAnalysisLauncher(PgIndex stmt, Index_restContext ctx, String location) {
        super(stmt, ctx, location);
    }

    /**
     * Analyzes the index definition to extract table dependencies.
     * Processes index columns, storage parameters and WHERE clauses.
     *
     * @param ctx  the parse tree context
     * @param meta metadata container for dependency resolution
     * @return set of object locations referenced in the index definition
     */
    @Override
    public Set<ObjectLocation> analyze(ParserRuleContext ctx, MetaContainer meta) {
        Set<ObjectLocation> depcies = new LinkedHashSet<>();
        Index_restContext rest = (Index_restContext) ctx;

        for (Index_columnContext c : rest.index_columns().index_column()) {
            depcies.addAll(analyzeTableChildVex(c.column, meta));

            Storage_parametersContext params = c.storage_parameters();
            if (params != null) {
                for (Storage_parameter_optionContext o : params.storage_parameter_option()) {
                    depcies.addAll(analyzeTableChildVex(o.vex(), meta));
                }
            }
        }

        if (rest.index_where() != null){
            depcies.addAll(analyzeTableChildVex(rest.index_where().vex(), meta));
        }

        return depcies;
    }
}
