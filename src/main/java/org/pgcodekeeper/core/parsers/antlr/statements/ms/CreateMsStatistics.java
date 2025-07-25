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
package org.pgcodekeeper.core.parsers.antlr.statements.ms;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.generated.TSQLParser.Create_statisticsContext;
import org.pgcodekeeper.core.parsers.antlr.generated.TSQLParser.Update_statistics_with_optionContext;
import org.pgcodekeeper.core.schema.AbstractSchema;
import org.pgcodekeeper.core.schema.GenericColumn;
import org.pgcodekeeper.core.schema.PgStatementContainer;
import org.pgcodekeeper.core.schema.ms.MsDatabase;
import org.pgcodekeeper.core.schema.ms.MsStatistics;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.Arrays;
import java.util.List;

/**
 * Parser for Microsoft SQL CREATE STATISTICS statements.
 * Handles statistics creation on table columns including filter conditions
 * and various statistics options like SAMPLE, NORECOMPUTE, AUTO_DROP, and INCREMENTAL.
 */
public final class CreateMsStatistics extends MsParserAbstract {

    private final Create_statisticsContext ctx;

    /**
     * Creates a parser for Microsoft SQL CREATE STATISTICS statements.
     *
     * @param ctx      the ANTLR parse tree context for the CREATE STATISTICS statement
     * @param db       the Microsoft SQL database schema being processed
     * @param settings parsing configuration settings
     */
    public CreateMsStatistics(Create_statisticsContext ctx, MsDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        var parentQualNameCtx = ctx.table_name_with_hint().qualified_name();
        var schemaCtx = parentQualNameCtx.schema;
        var parentCtx = parentQualNameCtx.name;
        var nameCtx = ctx.id();
        List<ParserRuleContext> ids = Arrays.asList(schemaCtx, nameCtx);
        AbstractSchema schema = getSchemaSafe(ids);
        addObjReference(Arrays.asList(schemaCtx, parentCtx), DbObjType.TABLE, null);

        var stat = new MsStatistics(nameCtx.getText());
        var whereCtx = ctx.index_where();
        if (whereCtx != null) {
            stat.setFilter(getFullCtxText(whereCtx.where));
        }

        for (var col : ctx.name_list_in_brackets().id()) {
            var colName = col.getText();
            stat.addCol(colName);
            stat.addDep(new GenericColumn(schemaCtx != null ? schemaCtx.getText() : null, parentCtx.getText(), colName,
                    DbObjType.COLUMN));
        }

        for (var option : ctx.update_statistics_with_option()) {
            pasreOption(stat, option);
        }

        PgStatementContainer st = getSafe(AbstractSchema::getStatementContainer, schema, parentCtx);
        addSafe(st, stat, Arrays.asList(schemaCtx, parentCtx, nameCtx));
    }

    private void pasreOption(MsStatistics stat, Update_statistics_with_optionContext option) {
        if (option.SAMPLE() != null || option.FULLSCAN() != null) {
            var valCtx = option.DECIMAL();
            var sb = new StringBuilder();
            if (valCtx != null) {
                String valType = option.PERCENT() != null ? " PERCENT" : " ROWS";
                sb.append(valCtx.getText()).append(valType);
            } else {
                sb.append("100 PERCENT");
            }
            stat.putOption("SAMPLE", sb.toString());
        } else if (option.NORECOMPUTE() != null) {
            stat.putOption("NORECOMPUTE", "NORECOMPUTE");
        } else if (option.AUTO_DROP() != null && option.on_off().ON() != null) {
            stat.putOption("AUTO_DROP", "ON");
        } else if (option.INCREMENTAL() != null && option.on_off().ON() != null) {
            stat.putOption("INCREMENTAL", "ON");
        }
    }

    @Override
    protected String getStmtAction() {
        var parentQualNameCtx = ctx.table_name_with_hint().qualified_name();
        return getStrForStmtAction(ACTION_CREATE, DbObjType.STATISTICS,
                Arrays.asList(parentQualNameCtx.schema, parentQualNameCtx.name, ctx.id()));
    }
}
