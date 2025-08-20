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
package org.pgcodekeeper.core.parsers.antlr.pg.statement;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.base.QNameParser;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.*;
import org.pgcodekeeper.core.parsers.antlr.base.statement.ParserAbstract;
import org.pgcodekeeper.core.schema.AbstractColumn;
import org.pgcodekeeper.core.schema.AbstractSchema;
import org.pgcodekeeper.core.schema.AbstractSequence;
import org.pgcodekeeper.core.schema.AbstractTable;
import org.pgcodekeeper.core.schema.pg.*;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.List;

/**
 * Parser for PostgreSQL CREATE FOREIGN TABLE statements.
 * <p>
 * This class handles parsing of foreign table definitions including columns,
 * partitioning information, server references, and foreign table options.
 * Foreign tables provide access to data stored in external data sources
 * through foreign data wrappers.
 */
public final class CreateForeignTable extends TableAbstract {

    private final Create_foreign_table_statementContext ctx;

    /**
     * Constructs a new CreateForeignTable parser.
     *
     * @param ctx      the CREATE FOREIGN TABLE statement context
     * @param db       the PostgreSQL database object
     * @param stream   the token stream for parsing
     * @param settings the ISettings object
     */
    public CreateForeignTable(Create_foreign_table_statementContext ctx, PgDatabase db, CommonTokenStream stream,
                              ISettings settings) {
        super(db, stream, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        List<ParserRuleContext> ids = getIdentifiers(ctx.name);
        String tableName = QNameParser.getFirstName(ids);
        AbstractSchema schema = getSchemaSafe(ids);
        AbstractTable table = defineTable(tableName, getSchemaNameSafe(ids));
        addSafe(schema, table, ids);

        for (AbstractColumn col : table.getColumns()) {
            AbstractSequence seq = ((PgColumn) col).getSequence();
            if (seq != null) {
                seq.setParent(schema);
            }
        }
    }

    private AbstractTable defineTable(String tableName, String schemaName) {
        Define_serverContext srvCtx = ctx.define_server();
        IdentifierContext srvName = srvCtx.identifier();
        Define_columnsContext colCtx = ctx.define_columns();
        Define_partitionContext partCtx = ctx.define_partition();

        AbstractPgTable table;

        if (colCtx != null) {
            table = fillForeignTable(srvCtx, new SimpleForeignPgTable(
                    tableName, srvName.getText()));
            fillColumns(colCtx, table, schemaName, null);
        } else {
            String partBound = ParserAbstract.getFullCtxText(partCtx.for_values_bound());
            table = fillForeignTable(srvCtx, new PartitionForeignPgTable(
                    tableName, srvName.getText(), partBound));

            fillTypeColumns(partCtx.list_of_type_column_def(), table, schemaName, null);
            addInherit(table, getIdentifiers(partCtx.parent_table));
        }
        addDepSafe(table, List.of(srvName), DbObjType.SERVER);

        return table;
    }

    private AbstractForeignTable fillForeignTable(Define_serverContext server, AbstractForeignTable table) {
        Define_foreign_optionsContext options = server.define_foreign_options();
        if (options != null) {
            for (Foreign_optionContext option : options.foreign_option()) {
                var opt = option.sconst();
                String value = opt == null ? null : opt.getText();
                fillOptionParams(value, option.col_label().getText(), false, table::addOption);
            }
        }
        return table;
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_CREATE, DbObjType.TABLE, getIdentifiers(ctx.name));
    }
}
