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
import org.pgcodekeeper.core.loader.FullAnalyze;
import org.pgcodekeeper.core.parsers.antlr.base.launcher.AbstractAnalysisLauncher;
import org.pgcodekeeper.core.parsers.antlr.pg.expr.Select;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.Select_stmtContext;
import org.pgcodekeeper.core.schema.PgObjLocation;
import org.pgcodekeeper.core.schema.meta.MetaContainer;
import org.pgcodekeeper.core.schema.meta.MetaUtils;
import org.pgcodekeeper.core.schema.pg.AbstractPgView;

import java.util.Set;

/**
 * Launcher for analyzing PostgreSQL view definitions.
 * Handles SELECT statement analysis for views and manages view metadata initialization.
 */
public class ViewAnalysisLauncher extends AbstractAnalysisLauncher {

    private FullAnalyze fullAnalyze;

    /**
     * Creates a view analyzer for PostgreSQL.
     *
     * @param stmt     the view statement to analyze
     * @param ctx      the SELECT statement context defining the view
     * @param location the source location identifier
     */
    public ViewAnalysisLauncher(AbstractPgView stmt, Select_stmtContext ctx, String location) {
        super(stmt, ctx, location);
    }

    public void setFullAnalyze(FullAnalyze fullAnalyze) {
        this.fullAnalyze = fullAnalyze;
    }

    @Override
    public Set<PgObjLocation> analyze(ParserRuleContext ctx, MetaContainer meta) {
        Select select = new Select(meta);
        select.setFullAnalyze(fullAnalyze);
        MetaUtils.initializeView(meta, getSchemaName(), stmt.getName(),
                select.analyze((Select_stmtContext) ctx));
        return select.getDependencies();
    }
}
