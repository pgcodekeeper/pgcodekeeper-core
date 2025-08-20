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
package org.pgcodekeeper.core.parsers.antlr.ms.launcher;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.parsers.antlr.base.launcher.AbstractAnalysisLauncher;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.Select_statementContext;
import org.pgcodekeeper.core.parsers.antlr.ms.expr.MsSelect;
import org.pgcodekeeper.core.schema.PgObjLocation;
import org.pgcodekeeper.core.schema.meta.MetaContainer;
import org.pgcodekeeper.core.schema.ms.MsView;

import java.util.Set;

/**
 * Launcher for analyzing Microsoft SQL view definitions.
 * Specialized for processing SELECT statements in view definitions.
 */
public class MsViewAnalysisLauncher extends AbstractAnalysisLauncher {

    /**
     * Creates a view analyzer for Microsoft SQL.
     *
     * @param stmt     the view statement to analyze
     * @param ctx      the SELECT statement context defining the view
     * @param location the source location identifier
     */
    public MsViewAnalysisLauncher(MsView stmt, Select_statementContext ctx,
            String location) {
        super(stmt, ctx, location);
    }

    @Override
    public Set<PgObjLocation> analyze(ParserRuleContext ctx, MetaContainer meta) {
        MsSelect select = new MsSelect(getSchemaName(), meta);
        select.analyze((Select_statementContext) ctx);
        return select.getDepcies();
    }
}
