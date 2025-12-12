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
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.parsers.antlr.base.launcher.AbstractAnalysisLauncher;
import org.pgcodekeeper.core.parsers.antlr.pg.expr.AbstractExprWithNmspc;
import org.pgcodekeeper.core.parsers.antlr.pg.expr.ValueExprWithNmspc;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.VexContext;
import org.pgcodekeeper.core.database.api.schema.GenericColumn;
import org.pgcodekeeper.core.database.api.schema.ObjectLocation;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.database.base.schema.meta.MetaContainer;

import java.util.Set;

/**
 * This class and all child classes contains PG statement, its contexts and
 * implementation of logic for launch the analysis of statement's contexts.
 */
public abstract class AbstractPgAnalysisLauncher extends AbstractAnalysisLauncher {

    protected AbstractPgAnalysisLauncher(AbstractStatement stmt, ParserRuleContext ctx, String location) {
        super(stmt, ctx, location);
    }

    /**
     * Sets up namespace for Constraint/Index expr analysis
     *
     * @param ctx
     *            expression context to analyze
     * @param meta
     *            database metadata
     *
     * @return dependencies from child expression
     */
    protected Set<ObjectLocation> analyzeTableChildVex(VexContext ctx, MetaContainer meta) {
        IStatement table = stmt.getParent();
        String schemaName = table.getParent().getName();
        String rawTableReference = table.getName();

        ValueExprWithNmspc valExprWithNmspc = new ValueExprWithNmspc(meta);
        valExprWithNmspc.addRawTableReference(
                new GenericColumn(schemaName, rawTableReference, DbObjType.TABLE));
        return analyze(ctx, valExprWithNmspc);
    }

    /**
     * Sets up namespace for Trigger/Rule expr/command analysis
     *
     * @param <T>
     *            analyzer type
     * @param ctx
     *            expression context to analyze
     * @param analyzer
     *            analyzer with database metadata
     *
     * @return dependencies from trigger/rule expression
     */
    protected <T extends ParserRuleContext> Set<ObjectLocation> analyzeTableChild (
            T ctx, AbstractExprWithNmspc<T> analyzer) {
        IStatement table = stmt.getParent();
        String schemaName = table.getParent().getName();
        String tableName = table.getName();
        GenericColumn implicitTable = new GenericColumn(schemaName, tableName, DbObjType.TABLE);
        analyzer.addReference("new", implicitTable);
        analyzer.addReference("old", implicitTable);
        return analyze(ctx, analyzer);
    }

    protected <T extends ParserRuleContext> Set<ObjectLocation> analyze(
            T ctx, AbstractExprWithNmspc<T> analyzer) {
        analyzer.analyze(ctx);
        return analyzer.getDependencies();
    }
}
