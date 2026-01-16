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
package org.pgcodekeeper.core.database.pg.parser.statement;

import java.util.List;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.base.parser.QNameParser;
import org.pgcodekeeper.core.database.pg.parser.generated.SQLParser.*;
import org.pgcodekeeper.core.database.pg.parser.launcher.PgStatisticsAnalysisLauncher;
import org.pgcodekeeper.core.database.pg.schema.*;
import org.pgcodekeeper.core.settings.ISettings;

/**
 * Parser for PostgreSQL CREATE STATISTICS statements.
 * <p>
 * This class handles parsing of extended statistics definitions including
 * statistics kinds (ndistinct, dependencies, mcv), target table references,
 * and column expressions. Extended statistics provide improved query planning
 * by collecting multi-column statistics.
 */
public final class PgCreateStatistics extends PgParserAbstract {

    private final Create_statistics_statementContext ctx;
    private final CommonTokenStream stream;

    /**
     * Constructs a new CreateStatistics parser.
     *
     * @param ctx      the CREATE STATISTICS statement context
     * @param db       the PostgreSQL database object
     * @param stream   the token stream for parsing expressions
     * @param settings the ISettings object
     */
    public PgCreateStatistics(Create_statistics_statementContext ctx, PgDatabase db, CommonTokenStream stream,
                            ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
        this.stream = stream;
    }

    @Override
    public void parseObject() {
        Schema_qualified_nameContext nameCtx = ctx.name;
        if (nameCtx == null) {
            return;
        }

        List<ParserRuleContext> ids = getIdentifiers(nameCtx);
        String name = QNameParser.getFirstName(ids);
        PgStatistics stat = new PgStatistics(name);

        parseStatistics(stat);

        addSafe(getSchemaSafe(ids), stat, ids);
    }

    /**
     * Parses statistics configuration from the statement context.
     *
     * @param stat the statistics object to populate with parsed data
     */
    public void parseStatistics(PgStatistics stat) {
        if (ctx.kind != null) {
            for (IdentifierContext k : ctx.kind.identifier()) {
                stat.addKind(k.getText());
            }
        }

        List<ParserRuleContext> tableIds = getIdentifiers(ctx.table_name);
        stat.setForeignSchema(QNameParser.getSchemaName(tableIds));
        stat.setForeignTable(QNameParser.getFirstName(tableIds));

        for (VexContext vex : ctx.vex()) {
            stat.addExpr(getExpressionText(vex, stream));
            db.addAnalysisLauncher(new PgStatisticsAnalysisLauncher(stat, vex, fileName));
        }

        addDepSafe(stat, tableIds, DbObjType.TABLE);
    }


    @Override
    protected String getStmtAction() {
        var nameCtx = ctx.name;
        if (nameCtx == null) {
            return ACTION_CREATE + ' ' + DbObjType.STATISTICS;
        }

        return getStrForStmtAction(ACTION_CREATE, DbObjType.STATISTICS, getIdentifiers(nameCtx));
    }
}
