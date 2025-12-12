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
package org.pgcodekeeper.core.parsers.antlr.ms.statement;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.*;
import org.pgcodekeeper.core.database.base.schema.AbstractIndex;
import org.pgcodekeeper.core.database.base.schema.AbstractSchema;
import org.pgcodekeeper.core.database.api.schema.GenericColumn;
import org.pgcodekeeper.core.database.base.schema.AbstractStatementContainer;
import org.pgcodekeeper.core.database.ms.schema.MsDatabase;
import org.pgcodekeeper.core.database.ms.schema.MsIndex;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.Arrays;
import java.util.List;

/**
 * Parser for Microsoft SQL CREATE INDEX statements.
 * Handles creation of regular indexes and columnstore indexes with support for
 * unique, clustered/non-clustered options, and various index configurations.
 */
public final class CreateMsIndex extends MsParserAbstract {

    private final Create_indexContext ctx;

    /**
     * Creates a parser for Microsoft SQL CREATE INDEX statements.
     *
     * @param ctx      the ANTLR parse tree context for the CREATE INDEX statement
     * @param db       the Microsoft SQL database schema being processed
     * @param settings parsing configuration settings
     */
    public CreateMsIndex(Create_indexContext ctx, MsDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        Index_nameContext indexNameCtx = ctx.index_name();
        IdContext schemaCtx = indexNameCtx.table_name.schema;
        IdContext tableCtx = indexNameCtx.table_name.name;
        IdContext nameCtx = indexNameCtx.name;
        List<ParserRuleContext> ids = Arrays.asList(schemaCtx, nameCtx);
        AbstractSchema schema = getSchemaSafe(ids);
        addObjReference(Arrays.asList(schemaCtx, tableCtx), DbObjType.TABLE, null);

        MsIndex ind = new MsIndex(nameCtx.getText());
        ind.setUnique(ctx.UNIQUE() != null);
        ClusteredContext cluster = ctx.clustered();
        ind.setClustered(cluster != null && cluster.CLUSTERED() != null);
        ind.setColumnstore(ctx.COLUMNSTORE() != null);
        var restCtx = ctx.index_rest();
        if (restCtx != null) {
            parseIndex(restCtx, ind, schemaCtx == null ? null : schemaCtx.getText(), tableCtx.getText());
        } else {
            parseColumnstoreIndex(ctx, ind, schemaCtx == null ? null : schemaCtx.getText(), tableCtx.getText());
        }

        AbstractStatementContainer cont = getSafe(AbstractSchema::getStatementContainer, schema, tableCtx);
        addSafe(cont, ind, Arrays.asList(schemaCtx, tableCtx, nameCtx));
    }

    private void parseColumnstoreIndex(Create_indexContext ctx, AbstractIndex index, String schema, String table) {
        var nameList = ctx.name_list_in_brackets();
        if (nameList != null) {
            for (IdContext col : nameList.id()) {
                index.addInclude(col.getText());
                index.addDependency(new GenericColumn(schema, table, col.getText(), DbObjType.COLUMN));
            }
        }
        var orderCols = ctx.order_cols;
        if (orderCols != null) {
            fillOrderCols((MsIndex) index, orderCols.column_name_list_with_order().column_with_order(), schema, table);
        }
        parseIndexOptions(index, ctx.index_where(), ctx.index_options(), ctx.dataspace());
    }

    @Override
    protected String getStmtAction() {
        Qualified_nameContext qualNameCtx = ctx.index_name().qualified_name();
        return getStrForStmtAction(ACTION_CREATE, DbObjType.INDEX,
                Arrays.asList(qualNameCtx.schema, qualNameCtx.name, ctx.index_name().name));
    }
}
