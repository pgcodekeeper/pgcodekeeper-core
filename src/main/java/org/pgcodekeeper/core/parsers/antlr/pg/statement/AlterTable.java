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
import org.pgcodekeeper.core.DangerStatement;
import org.pgcodekeeper.core.exception.UnresolvedReferenceException;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.base.AntlrUtils;
import org.pgcodekeeper.core.parsers.antlr.base.QNameParser;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.*;
import org.pgcodekeeper.core.parsers.antlr.pg.launcher.VexAnalysisLauncher;
import org.pgcodekeeper.core.schema.*;
import org.pgcodekeeper.core.schema.pg.*;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Parser for PostgreSQL ALTER TABLE statements.
 * <p>
 * This class handles parsing of table alterations including adding/dropping columns,
 * modifying constraints, changing table properties, managing triggers and rules,
 * setting table ownership, and handling Greenplum partition templates.
 */
public final class AlterTable extends TableAbstract {

    private final Alter_table_statementContext ctx;
    private final String tablespace;
    private final CommonTokenStream stream;

    /**
     * Constructs a new AlterTable parser.
     *
     * @param ctx        the ALTER TABLE statement context
     * @param db         the PostgreSQL database object
     * @param tablespace the default tablespace name
     * @param stream     the token stream for parsing
     * @param settings   the ISettings object
     */
    public AlterTable(Alter_table_statementContext ctx, PgDatabase db, String tablespace, CommonTokenStream stream,
                      ISettings settings) {
        super(db, stream, settings);
        this.ctx = ctx;
        this.tablespace = tablespace;
        this.stream = stream;
    }

    @Override
    public void parseObject() {
        List<ParserRuleContext> ids = getIdentifiers(ctx.name);
        AbstractSchema schema = getSchemaSafe(ids);
        ParserRuleContext nameCtx = QNameParser.getFirstNameCtx(ids);
        AbstractPgTable tabl;

        PgObjLocation loc = addObjReference(ids, DbObjType.TABLE, ACTION_ALTER);

        for (Table_actionContext tablAction : ctx.table_action()) {
            IdentifierContext column = tablAction.column;
            Column_actionContext colAction = tablAction.column_action();

            if (column != null && tablAction.DROP() != null) {
                loc.setWarning(DangerStatement.DROP_COLUMN);
            } else if (colAction != null && colAction.data_type() != null) {
                loc.setWarning(DangerStatement.ALTER_COLUMN);
            }

            if (tablAction.owner_to() != null || tablAction.index_name != null) {
                fillRelationAction(schema, nameCtx, tablAction);
                continue;
            }

            // everything else requires a real table, so fail immediately
            tabl = (AbstractPgTable) getSafe(AbstractSchema::getTable, schema, nameCtx);

            if (tablAction.tabl_constraint != null) {
                var tableConstraint = tablAction.tabl_constraint;
                if (tableConstraint.constr_body().NULL() != null) {
                    addNotNullTableConstraint(tableConstraint, tabl);
                    return;
                }
                IdentifierContext conNameCtx = tableConstraint.identifier();
                AbstractConstraint constr = parseAlterTableConstraint(tablAction,
                        createTableConstraintBlank(tableConstraint),
                        getSchemaNameSafe(ids), nameCtx.getText(), fileName);

                if (!constr.getName().isEmpty()) {
                    addSafe(tabl, constr, Arrays.asList(
                            QNameParser.getSchemaNameCtx(ids), nameCtx, conNameCtx));
                } else {
                    doSafe(AbstractPgTable::addConstraint, tabl, constr);
                }
            }

            if (tablAction.drop_constraint() != null) {
                addObjReference(Arrays.asList(QNameParser.getSchemaNameCtx(ids), nameCtx,
                                tablAction.drop_constraint().constraint_name),
                        DbObjType.CONSTRAINT, ACTION_DROP);
            }

            if (isRefMode()) {
                continue;
            }

            Table_column_definitionContext def = tablAction.table_column_definition();
            if (def != null) {
                addColumn(def.identifier().getText(), def.data_type(), def.storage_option(),
                        def.collate_identifier(), def.compression_identifier(), def.constraint_common(),
                        def.encoding_identifier(), def.define_foreign_options(), tabl, getSchemaNameSafe(ids));
            }

            if (column != null && colAction != null) {
                PgColumn col;
                if (tabl.getInherits().isEmpty()) {
                    col = (PgColumn) getSafe(AbstractTable::getColumn, tabl, column);
                } else {
                    String colName = column.getText();
                    col = (PgColumn) tabl.getColumn(colName);
                    if (col == null) {
                        col = new PgColumn(colName);
                        col.setInherit(true);
                        tabl.addColumn(col);
                    }
                }
                parseColumnAction(schema, col, colAction);
            }

            if (tablAction.WITHOUT() != null && tablAction.OIDS() != null) {
                tabl.setHasOids(false);
            } else if (tablAction.WITH() != null && tablAction.OIDS() != null) {
                tabl.setHasOids(true);
            }

            if (tablAction.TRIGGER() != null) {
                createTrigger(tabl, tablAction, ids);
            }

            if (tablAction.RULE() != null) {
                createRule(tabl, tablAction);
            }

            if (tablAction.SECURITY() != null && tabl instanceof AbstractRegularTable regTable) {
                // since 9.5 PostgreSQL
                if (tablAction.FORCE() != null) {
                    regTable.setForceSecurity(tablAction.NO() == null);
                } else {
                    regTable.setRowSecurity(tablAction.ENABLE() != null);
                }
            }
        }
        var alterPartition = ctx.alter_partition_gp();
        if (alterPartition != null && !isRefMode()) {
            tabl = (PartitionGpTable) getSafe(AbstractSchema::getTable, schema, nameCtx);
            parseGpPartitionTemplate((PartitionGpTable) tabl, alterPartition, stream);
        }
    }

    private void fillRelationAction(AbstractSchema schema, ParserRuleContext nameCtx, Table_actionContext tablAction) {
        IRelation r = getSafe(AbstractSchema::getRelation, schema, nameCtx);
        if (tablAction.owner_to() != null) {
            if (r instanceof PgStatement st) {
                fillOwnerTo(tablAction.owner_to().user_name().identifier(), st);
            }
            return;
        }

        if (r instanceof PgStatementContainer cont) {
            var indexNameCtx = tablAction.index_name;
            ParserRuleContext indexName = QNameParser.getFirstNameCtx(getIdentifiers(indexNameCtx));
            AbstractConstraint constr = cont.getConstraint(indexName.getText());
            if (constr != null) {
                if (constr instanceof PgConstraintPk pk) {
                    doSafe(PgConstraintPk::setClustered, pk, true);
                } else if (!isRefMode()) {
                    throw new IllegalArgumentException(Messages.Constraint_WarningMismatchedConstraintTypeForClusterOn);
                }
            } else {
                AbstractIndex index = getSafe(PgStatementContainer::getIndex, cont, indexName);
                doSafe(AbstractIndex::setClustered, index, true);
            }
        }

    }

    /**
     * Parses Greenplum partition template specifications from ALTER PARTITION clause.
     *
     * @param tabl           the Greenplum partitioned table to modify
     * @param alterPartition the ALTER PARTITION context
     * @param stream         the token stream for parsing
     */
    public static void parseGpPartitionTemplate(PartitionGpTable tabl, Alter_partition_gpContext alterPartition,
                                                CommonTokenStream stream) {
        // ALTER PARTITION partition_name clause
        String partitionName = null;
        var alterPartitionClause = alterPartition.alter_partition_gp_name();
        if (!alterPartitionClause.isEmpty() && alterPartitionClause.get(0).identifier() != null) {
            partitionName = alterPartitionClause.get(0).identifier().getText();
        }
        PartitionTemplateContainer template = new PartitionTemplateContainer(partitionName);

        // SET SUBPARTITION TEMPLATE clause
        var partitionAction = alterPartition.partition_gp_action();
        var subpartitions = partitionAction.template_spec().part_element();
        for (var subpartElem : subpartitions) {
            template.setSubElems(getFullCtxText(subpartElem),
                    AntlrUtils.normalizeWhitespaceUnquoted(subpartElem, stream));
        }
        if (template.hasSubElements()) {
            tabl.addTemplate(template);
        }
    }

    private void parseColumnAction(AbstractSchema schema, PgColumn col,
                                   Column_actionContext colAction) {
        // column statistics
        Set_statisticsContext statistics = colAction.set_statistics();
        if (statistics != null) {
            col.setStatistics(Integer.valueOf(statistics.signediconst().getText()));
        }

        // column not null constraint
        if (colAction.set != null) {
            col.setNotNull(true);
        }

        // column default
        Set_def_columnContext def = colAction.set_def_column();
        if (def != null) {
            VexContext expCtx = def.vex();
            col.setDefaultValue(getFullCtxTextWithCheckNewLines(expCtx));
            db.addAnalysisLauncher(new VexAnalysisLauncher(col, expCtx, fileName));
        }

        // column options
        Storage_parametersContext param = colAction.storage_parameters();
        if (param != null) {
            for (Storage_parameter_optionContext option : param.storage_parameter_option()) {
                VexContext opt = option.vex();
                String value = opt == null ? null : opt.getText();
                fillOptionParams(value, option.storage_parameter_name().getText(), false, col::addOption);
            }
        }

        // foreign options
        Define_foreign_optionsContext fOptions = colAction.define_foreign_options();
        if (fOptions != null) {
            for (Foreign_optionContext option : fOptions.foreign_option()) {
                var opt = option.sconst();
                String value = opt == null ? null : opt.getText();
                fillOptionParams(value, option.col_label().getText(), false, col::addForeignOption);
            }
        }

        // column storage
        Storage_optionContext sOptions = colAction.storage_option();
        if (sOptions != null) {
            col.setStorage(sOptions.getText());
        }

        // since 10 PostgreSQL
        Identity_bodyContext identity = colAction.identity_body();
        if (identity != null) {
            String name = null;
            for (Sequence_bodyContext body : identity.sequence_body()) {
                if (body.NAME() != null) {
                    name = QNameParser.getFirstName(getIdentifiers(body.name));
                }
            }
            PgSequence sequence = new PgSequence(name);
            sequence.setDataType(col.getType());
            CreateSequence.fillSequence(sequence, identity.sequence_body());

            var table = col.getParent();
            if (table instanceof AbstractRegularTable regTable) {
                sequence.setLogged(regTable.isLogged());
            }

            col.setSequence(sequence);
            sequence.setParent(schema);
            col.setIdentityType(identity.ALWAYS() != null ? "ALWAYS" : "BY DEFAULT");
        }
    }

    private void createTrigger(AbstractPgTable tabl, Table_actionContext tablAction, List<ParserRuleContext> ids) {
        var triggerNameCtx = tablAction.trigger_name;
        if (triggerNameCtx == null) {
            return;
        }

        TriggerState triggerState = null;
        if (tablAction.DISABLE() != null) {
            triggerState = TriggerState.DISABLE;
        } else if (tablAction.ENABLE() != null) {
            if (tablAction.REPLICA() != null) {
                triggerState = TriggerState.ENABLE_REPLICA;
            } else if (tablAction.ALWAYS() != null) {
                triggerState = TriggerState.ENABLE_ALWAYS;
            } else {
                triggerState = TriggerState.ENABLE;
            }
        }

        String triggerName = triggerNameCtx.getText();
        if (tabl.getTrigger(triggerName) == null) {
            if (!tabl.hasInherits()) {
                throw new UnresolvedReferenceException(Messages.AlterTriggerError, triggerNameCtx.getStop());
            }
            tabl.putTriggerState(triggerName, triggerState);
        } else {
            PgTrigger trigger = (PgTrigger) getSafe(PgStatementContainer::getTrigger, tabl,
                    triggerNameCtx);
            trigger.setTriggerState(triggerState);
        }
        var idsCopy = new ArrayList<>(ids);
        idsCopy.add(triggerNameCtx);
        addObjReference(idsCopy, DbObjType.TRIGGER, null);
    }

    private void createRule(AbstractPgTable tabl, Table_actionContext tablAction) {
        PgRule rule = getSafe(AbstractTable::getRule, tabl, getIdentifiers(tablAction.rewrite_rule_name).get(0));
        if (rule != null) {
            if (tablAction.DISABLE() != null) {
                rule.setEnabledState("DISABLE");
            } else if (tablAction.ENABLE() != null) {
                if (tablAction.REPLICA() != null) {
                    rule.setEnabledState("ENABLE REPLICA");
                } else if (tablAction.ALWAYS() != null) {
                    rule.setEnabledState("ENABLE ALWAYS");
                }
            }
        }
    }

    public AbstractConstraint parseAlterTableConstraint(Table_actionContext tableAction,
                                                        PgConstraint constrBlank, String schemaName, String tableName, String location) {
        processTableConstraintBlank(tableAction.tabl_constraint, constrBlank,
                schemaName, tableName, tablespace, location);
        return constrBlank;
    }

    @Override
    protected PgObjLocation fillQueryLocation(ParserRuleContext ctx) {
        PgObjLocation loc = super.fillQueryLocation(ctx);
        for (Table_actionContext tablAction : ((Schema_alterContext) ctx)
                .alter_table_statement().table_action()) {
            IdentifierContext column = tablAction.column;
            Column_actionContext colAction = tablAction.column_action();

            if (column != null && tablAction.DROP() != null) {
                loc.setWarning(DangerStatement.DROP_COLUMN);
            } else if (colAction != null && colAction.data_type() != null) {
                loc.setWarning(DangerStatement.ALTER_COLUMN);
            }
        }
        return loc;
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_ALTER, DbObjType.TABLE, getIdentifiers(ctx.name));
    }
}
