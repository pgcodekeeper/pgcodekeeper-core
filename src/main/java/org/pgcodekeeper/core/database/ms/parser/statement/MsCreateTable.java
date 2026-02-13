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
package org.pgcodekeeper.core.database.ms.parser.statement;

import java.util.*;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.ms.parser.generated.TSQLParser.*;
import org.pgcodekeeper.core.database.ms.parser.launcher.MsExpressionAnalysisLauncher;
import org.pgcodekeeper.core.database.ms.schema.*;
import org.pgcodekeeper.core.settings.ISettings;

/**
 * Parser for Microsoft SQL CREATE TABLE statements.
 * Handles table creation including columns, constraints, indexes, system versioning,
 * tablespaces, and various Microsoft SQL-specific features.
 */
public final class MsCreateTable extends MsTableAbstract {

    private final Create_tableContext ctx;

    private final boolean ansiNulls;

    /**
     * Creates a parser for Microsoft SQL CREATE TABLE statements.
     *
     * @param ctx       the ANTLR parse tree context for the CREATE TABLE statement
     * @param db        the Microsoft SQL database schema being processed
     * @param ansiNulls the ANSI_NULLS setting for the table
     * @param settings  parsing configuration settings
     */
    public MsCreateTable(Create_tableContext ctx, MsDatabase db, boolean ansiNulls, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
        this.ansiNulls = ansiNulls;
    }

    @Override
    public void parseObject() {
        IdContext nameCtx = ctx.qualified_name().name;
        String tableName = nameCtx.getText();

        MsTable table = new MsTable(tableName);

        List<ParserRuleContext> ids = Arrays.asList(ctx.qualified_name().schema, nameCtx);
        addSafe(getSchemaSafe(ids), table, ids);

        table.setAnsiNulls(ansiNulls);

        var dataSpaceCtx = ctx.dataspace();
        if (dataSpaceCtx != null) {
            table.setTablespace(buildDataSpace(dataSpaceCtx));
        }

        if (ctx.textimage != null) {
            table.setTextImage(ctx.textimage.getText());
        }

        if (ctx.filestream != null) {
            table.setFileStream(ctx.filestream.getText());
        }

        for (Table_optionsContext options : ctx.table_options()) {
            fillOptions(table, options.index_option());
        }

        for (Table_element_extendedContext colCtx : ctx.table_elements_extended().table_element_extended()) {
            fillColumn(colCtx, table);
        }
    }

    private void fillColumn(Table_element_extendedContext tableElementCtx, MsTable table) {
        IdContext schemaCtx = ctx.qualified_name().schema;
        IdContext tableCtx = ctx.qualified_name().name;
        String schemaName = schemaCtx == null ? null : schemaCtx.getText();
        String tableName = tableCtx.getText();

        Column_defContext colCtx = tableElementCtx.column_def();
        Table_indexContext indCtx;
        Table_constraintContext constrCtx;
        Period_for_system_timeContext periodCtx;

        if (colCtx != null) {
            parseColumnDef(colCtx, table);
        } else if ((constrCtx = tableElementCtx.table_constraint()) != null) {
            table.addChild(getMsConstraint(constrCtx, schemaName, tableName));
        } else if ((indCtx = tableElementCtx.table_index()) != null) {
            parseTableIndex(indCtx, table, schemaName, tableName);
        } else if ((periodCtx = tableElementCtx.period_for_system_time()) != null) {
            var startCol = getSafe(MsTable::getColumn, table, periodCtx.start_col_name);
            var endCol = getSafe(MsTable::getColumn, table, periodCtx.end_col_name);

            table.setPeriodStartCol(startCol);
            table.setPeriodEndCol(endCol);
        } else {
            // TODO add COLUMN_SET support
        }
    }

    private void parseTableIndex(Table_indexContext indCtx, MsTable table, String schemaName, String tableName) {
        MsIndex index = new MsIndex(indCtx.ind_name.getText());

        var restCtx = indCtx.index_rest();
        if (restCtx != null) {
            index.setUnique(indCtx.UNIQUE() != null);
            ClusteredContext cluster = indCtx.clustered();
            index.setClustered(cluster != null && cluster.CLUSTERED() != null);
            parseIndex(restCtx, index, schemaName, tableName);
        } else {
            var columnstoreIndCtx = indCtx.columnstore_index();
            index.setColumnstore(true);
            index.setClustered(columnstoreIndCtx.CLUSTERED() != null);
            parseColumnstoreIndex(indCtx.columnstore_index(), index, schemaName, tableName);
            parseIndexOptions(index, indCtx.index_where(), indCtx.index_options(), ctx.dataspace());
            // if user didn't set it, database will be set it by default
            if (null == index.getOptions().get("DATA_COMPRESSION")) {
                index.addOption("DATA_COMPRESSION", "COLUMNSTORE");
            }
        }

        if (index.getTablespace() == null) {
            index.setTablespace(table.getTablespace());
        }

        table.addChild(index);
    }

    private void parseColumnDef(Column_defContext colCtx, MsTable table) {
        if (colCtx == null) {
            return;
        }

        MsColumn col = new MsColumn(colCtx.id().getText());
        Data_typeContext dataType;
        ExpressionContext expr;
        if ((dataType = colCtx.data_type()) != null) {
            addTypeDepcy(dataType, col);
            col.setType(getType(dataType));
        } else if ((expr = colCtx.expression()) != null) {
            col.setExpression(getFullCtxTextWithCheckNewLines(expr));
            db.addAnalysisLauncher(new MsExpressionAnalysisLauncher(col, expr, fileName));
        } else {
            // TODO add xml analyzator
        }

        for (Column_optionContext option : colCtx.column_option()) {
            fillColumnOption(option, col, table);
        }

        table.addColumn(col);
    }

    private void parseColumnstoreIndex(Columnstore_indexContext ctx, MsIndex index, String schema, String table) {
        var nameList = ctx.name_list_in_brackets();
        if (nameList != null) {
            for (IdContext col : nameList.id()) {
                index.addInclude(col.getText());
                index.addDependency(new ObjectReference(schema, table, col.getText(), DbObjType.COLUMN));
            }
        }
        var orderCols = ctx.order_cols;
        if (orderCols != null) {
            fillOrderCols(index, orderCols.column_name_list_with_order().column_with_order(), schema, table);
        }
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_CREATE, DbObjType.TABLE, ctx.qualified_name());
    }
}
