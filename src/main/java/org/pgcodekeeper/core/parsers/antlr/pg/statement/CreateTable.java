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
package org.pgcodekeeper.core.parsers.antlr.pg.statement;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.pg.schema.*;
import org.pgcodekeeper.core.parsers.antlr.base.AntlrTask;
import org.pgcodekeeper.core.parsers.antlr.base.AntlrTaskManager;
import org.pgcodekeeper.core.parsers.antlr.base.AntlrUtils;
import org.pgcodekeeper.core.parsers.antlr.base.QNameParser;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.*;
import org.pgcodekeeper.core.database.base.schema.AbstractColumn;
import org.pgcodekeeper.core.database.base.schema.AbstractSchema;
import org.pgcodekeeper.core.database.base.schema.AbstractTable;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.List;
import java.util.Queue;

/**
 * Parser for PostgreSQL CREATE TABLE statements.
 * <p>
 * This class handles parsing of table definitions including regular tables,
 * partitioned tables, typed tables, and Greenplum-specific table types.
 * It processes columns, constraints, inheritance, storage parameters, and
 * table-specific options like tablespace, access method, and OIDs.
 */
public final class CreateTable extends TableAbstract {
    private final Create_table_statementContext ctx;
    private final String tablespace;
    private final String accessMethod;
    private final String oids;
    private final CommonTokenStream stream;
    private final Queue<AntlrTask<?>> antlrTasks;

    /**
     * Constructs a new CreateTable parser.
     *
     * @param ctx          the CREATE TABLE statement context
     * @param db           the PostgreSQL database object
     * @param tablespace   the default tablespace name
     * @param accessMethod the default access method
     * @param oids         the default OIDs setting
     * @param stream       the token stream for parsing
     * @param settings     the ISettings object
     */
    public CreateTable(Create_table_statementContext ctx, PgDatabase db,
                       String tablespace, String accessMethod, String oids,
                       CommonTokenStream stream, ISettings settings, Queue<AntlrTask<?>> antlrTasks) {
        super(db, stream, settings);
        this.ctx = ctx;
        this.tablespace = tablespace;
        this.accessMethod = accessMethod;
        this.oids = oids;
        this.stream = stream;
        this.antlrTasks = antlrTasks;
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
                if (table instanceof PgAbstractRegularTable regTable) {
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

        PgAbstractTable table;

        if (typeCtx != null) {
            table = defineType(typeCtx, tableName, schemaName);
        } else if (colCtx != null) {
            PgAbstractRegularTable abstractRegTable;
            if (ctx.partition_gp() != null) {
                abstractRegTable = new GpPartitionTable(tableName);
            } else {
                abstractRegTable = new PgSimpleTable(tableName);
            }
            table = fillRegularTable(abstractRegTable);
            fillColumns(colCtx, table, schemaName, tablespace);
        } else {
            String partBound = getFullCtxText(partCtx.for_values_bound());
            table = fillRegularTable(new PgPartitionTable(tableName, partBound));
            fillTypeColumns(partCtx.list_of_type_column_def(), table, schemaName, tablespace);
            addInherit(table, getIdentifiers(partCtx.parent_table));
        }

        return table;
    }

    private PgTypedTable defineType(Define_typeContext typeCtx, String tableName,
                                      String schemaName) {
        Data_typeContext typeName = typeCtx.type_name;
        String ofType = getTypeName(typeName);
        PgTypedTable table = new PgTypedTable(tableName, ofType);
        fillTypeColumns(typeCtx.list_of_type_column_def(), table, schemaName, tablespace);
        addTypeDepcy(typeName, table);
        fillRegularTable(table);
        return table;
    }

    private PgAbstractRegularTable fillRegularTable(PgAbstractRegularTable table) {
        if (ctx.table_space() != null) {
            table.setTablespace(ctx.table_space().identifier().getText());
        } else if (tablespace != null) {
            table.setTablespace(tablespace);
        }

        String distribution = parseDistribution(ctx.distributed_clause());
        table.setDistribution(distribution);

        if (table instanceof GpPartitionTable partTable) {
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

    private void parseOptions(List<Storage_parameter_optionContext> options, PgAbstractRegularTable table) {
        for (Storage_parameter_optionContext option : options) {
            Storage_parameter_nameContext key = option.storage_parameter_name();
            List<Col_labelContext> optionIds = key.col_label();
            VexContext valueCtx = option.vex();
            String value = valueCtx == null ? "" : valueCtx.getText();
            String optionText = key.getText();
            if ("OIDS".equalsIgnoreCase(optionText)) {
                if ("TRUE".equalsIgnoreCase(value) || "'TRUE'".equalsIgnoreCase(value)) {
                    table.setHasOids(true);
                }
            } else if ("toast".equals(QNameParser.getSecondName(optionIds))) {
                fillOptionParams(value, QNameParser.getFirstName(optionIds), true, table::addOption);
            } else {
                fillOptionParams(value, optionText, false, table::addOption);
            }
        }
    }

    @Override
    protected void fillColNotNull(AbstractTable table, Constraint_commonContext tblConstrCtx,
                                  Schema_qualified_nameContext colNameCtx) {
        AntlrTaskManager.submit(antlrTasks, () -> colNameCtx, colCtx -> {
            var col = (PgColumn) getSafe(AbstractTable::getColumn, table, colNameCtx);
            if (col != null) {
                fillColNotNull(col, table, tblConstrCtx);
            }
        });
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_CREATE, DbObjType.TABLE, getIdentifiers(ctx.name));
    }
}
