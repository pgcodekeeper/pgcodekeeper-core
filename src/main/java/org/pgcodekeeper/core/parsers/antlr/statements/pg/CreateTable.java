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
package org.pgcodekeeper.core.parsers.antlr.statements.pg;

import java.util.List;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.AntlrUtils;
import org.pgcodekeeper.core.parsers.antlr.QNameParser;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.Col_labelContext;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.Create_table_statementContext;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.Data_typeContext;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.Define_columnsContext;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.Define_partitionContext;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.Define_tableContext;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.Define_typeContext;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.Partition_byContext;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.Storage_parameter_nameContext;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.Storage_parameter_oidContext;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.Storage_parameter_optionContext;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.VexContext;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.With_storage_parameterContext;
import org.pgcodekeeper.core.schema.AbstractColumn;
import org.pgcodekeeper.core.schema.AbstractSchema;
import org.pgcodekeeper.core.schema.AbstractTable;
import org.pgcodekeeper.core.schema.pg.AbstractPgTable;
import org.pgcodekeeper.core.schema.pg.AbstractRegularTable;
import org.pgcodekeeper.core.schema.pg.PartitionGpTable;
import org.pgcodekeeper.core.schema.pg.PartitionPgTable;
import org.pgcodekeeper.core.schema.pg.PgColumn;
import org.pgcodekeeper.core.schema.pg.PgDatabase;
import org.pgcodekeeper.core.schema.pg.PgSequence;
import org.pgcodekeeper.core.schema.pg.SimplePgTable;
import org.pgcodekeeper.core.schema.pg.TypedPgTable;
import org.pgcodekeeper.core.settings.ISettings;

public final class CreateTable extends TableAbstract {
    private final Create_table_statementContext ctx;
    private final String tablespace;
    private final String accessMethod;
    private final String oids;
    private final CommonTokenStream stream;


    public CreateTable(Create_table_statementContext ctx, PgDatabase db,
            String tablespace, String accessMethod, String oids, CommonTokenStream stream, ISettings settings) {
        super(db, stream, settings);
        this.ctx = ctx;
        this.tablespace = tablespace;
        this.accessMethod = accessMethod;
        this.oids = oids;
        this.stream = stream;
    }

    @Override
    public void parseObject() {
        List<ParserRuleContext> ids = getIdentifiers(ctx.name);
        String tableName = QNameParser.getFirstName(ids);
        String schemaName = getSchemaNameSafe(ids);
        AbstractSchema schema = getSchemaSafe(ids);
        AbstractTable table = defineTable(tableName, schemaName);
        addSafe(schema, table, ids);

        for (AbstractColumn col : table.getColumns()) {
            PgSequence seq = ((PgColumn) col).getSequence();
            if (seq != null) {
                if (table instanceof AbstractRegularTable regTable) {
                    seq.setLogged(regTable.isLogged());
                }
                seq.setParent(schema);
            }
        }
    }

    private AbstractTable defineTable(String tableName, String schemaName) {
        Define_tableContext tabCtx = ctx.define_table();
        Define_columnsContext colCtx = tabCtx.define_columns();
        Define_typeContext typeCtx = tabCtx.define_type();
        Define_partitionContext partCtx = tabCtx.define_partition();

        AbstractPgTable table;

        if (typeCtx != null) {
            table = defineType(typeCtx, tableName, schemaName);
        } else if (colCtx != null) {
            AbstractRegularTable abstractRegTable;
            if (ctx.partition_gp() != null) {
                abstractRegTable = new PartitionGpTable(tableName);
            } else {
                abstractRegTable = new SimplePgTable(tableName);
            }
            table = fillRegularTable(abstractRegTable);
            fillColumns(colCtx, table, schemaName, tablespace);
        } else {
            String partBound = getFullCtxText(partCtx.for_values_bound());
            table = fillRegularTable(new PartitionPgTable(tableName, partBound));
            fillTypeColumns(partCtx.list_of_type_column_def(), table, schemaName, tablespace);
            addInherit(table, getIdentifiers(partCtx.parent_table));
        }

        return table;
    }

    private TypedPgTable defineType(Define_typeContext typeCtx, String tableName,
            String schemaName) {
        Data_typeContext typeName = typeCtx.type_name;
        String ofType = getTypeName(typeName);
        TypedPgTable table = new TypedPgTable(tableName, ofType);
        fillTypeColumns(typeCtx.list_of_type_column_def(), table, schemaName, tablespace);
        addTypeDepcy(typeName, table);
        fillRegularTable(table);
        return table;
    }

    private AbstractRegularTable fillRegularTable(AbstractRegularTable table) {
        if (ctx.table_space() != null) {
            table.setTablespace(ctx.table_space().identifier().getText());
        } else if (tablespace != null) {
            table.setTablespace(tablespace);
        }

        String distribution = parseDistribution(ctx.distributed_clause());
        table.setDistribution(distribution);

        if (table instanceof PartitionGpTable partTable) {
            var partitionGP = ctx.partition_gp();
            partTable.setPartitionGpBound(getFullCtxText(partitionGP),
                    AntlrUtils.normalizeWhitespaceUnquoted(partitionGP, stream));
        }

        boolean explicitOids = false;
        Storage_parameter_oidContext storage = ctx.storage_parameter_oid();
        if (storage != null) {
            With_storage_parameterContext parameters = storage.with_storage_parameter();
            if (parameters != null) {
                parseOptions(parameters.storage_parameters().storage_parameter_option(), table);
            }
            if (storage.WITHOUT() != null) {
                table.setHasOids(false);
                explicitOids = true;
            } else if (storage.WITH() != null) {
                table.setHasOids(true);
                explicitOids = true;
            }
        }

        if (!explicitOids && oids != null) {
            table.setHasOids(true);
        }

        if (ctx.UNLOGGED() != null) {
            table.setLogged(false);
        }

        Partition_byContext part = ctx.partition_by();
        if (part != null) {
            table.setPartitionBy(getFullCtxText(part.partition_method()));
        }

        // table access method for partitioned tables is not supported for PG
        if (ctx.USING() != null) {
            table.setMethod(ctx.identifier().getText());
        } else if (accessMethod != null && (part == null || distribution != null)) {
            table.setMethod(accessMethod);
        }

        return table;
    }

    private void parseOptions(List<Storage_parameter_optionContext> options, AbstractRegularTable table){
        for (Storage_parameter_optionContext option : options){
            Storage_parameter_nameContext key = option.storage_parameter_name();
            List<Col_labelContext> optionIds = key.col_label();
            VexContext valueCtx = option.vex();
            String value = valueCtx == null ? "" : valueCtx.getText();
            String optionText = key.getText();
            if ("OIDS".equalsIgnoreCase(optionText)) {
                if ("TRUE".equalsIgnoreCase(value) || "'TRUE'".equalsIgnoreCase(value)) {
                    table.setHasOids(true);
                }
            } else if("toast".equals(QNameParser.getSecondName(optionIds))) {
                fillOptionParams(value, QNameParser.getFirstName(optionIds), true, table::addOption);
            } else {
                fillOptionParams(value, optionText, false, table::addOption);
            }
        }
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_CREATE, DbObjType.TABLE, getIdentifiers(ctx.name));
    }
}
