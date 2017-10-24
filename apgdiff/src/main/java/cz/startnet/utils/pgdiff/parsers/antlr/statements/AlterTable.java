package cz.startnet.utils.pgdiff.parsers.antlr.statements;

import java.util.List;

import cz.startnet.utils.pgdiff.parsers.antlr.QNameParser;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Alter_table_statementContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Foreign_optionContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.IdentifierContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Identity_bodyContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Sequence_bodyContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Storage_parameter_optionContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Table_actionContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Table_column_definitionContext;
import cz.startnet.utils.pgdiff.parsers.antlr.expr.ValueExpr;
import cz.startnet.utils.pgdiff.parsers.antlr.rulectx.Vex;
import cz.startnet.utils.pgdiff.schema.PgColumn;
import cz.startnet.utils.pgdiff.schema.PgConstraint;
import cz.startnet.utils.pgdiff.schema.PgDatabase;
import cz.startnet.utils.pgdiff.schema.PgIndex;
import cz.startnet.utils.pgdiff.schema.PgRule;
import cz.startnet.utils.pgdiff.schema.PgSchema;
import cz.startnet.utils.pgdiff.schema.PgSequence;
import cz.startnet.utils.pgdiff.schema.PgStatement;
import cz.startnet.utils.pgdiff.schema.PgTable;
import cz.startnet.utils.pgdiff.schema.RegularPgTable;

public class AlterTable extends ParserAbstract {

    private final Alter_table_statementContext ctx;
    public AlterTable(Alter_table_statementContext ctx, PgDatabase db) {
        super(db);
        this.ctx = ctx;
    }

    @Override
    public PgStatement getObject() {
        List<IdentifierContext> ids = ctx.name.identifier();
        PgSchema schema = getSchemaSafe(ids, db.getDefaultSchema());
        IdentifierContext nameCtx = QNameParser.getFirstNameCtx(ids);
        PgTable tabl = null;

        for (Table_actionContext tablAction : ctx.table_action()) {
            // for owners try to get any relation, fail if the last attempt fails
            if (tablAction.owner_to() != null) {
                PgStatement st = null;
                String name = nameCtx.getText();
                if ((st = schema.getTable(name)) != null) {
                    fillOwnerTo(tablAction.owner_to(), st);
                } else if ((st = schema.getSequence(name)) != null) {
                    fillOwnerTo(tablAction.owner_to(), st);
                } else if ((st = getSafe(schema::getView, nameCtx)) != null) {
                    fillOwnerTo(tablAction.owner_to(), st);
                }
                continue;
            }

            // everything else requires a real table, so fail immediately
            tabl = getSafe(schema::getTable, QNameParser.getFirstNameCtx(ids));
            if (tablAction.table_column_definition() != null) {
                Table_column_definitionContext column = tablAction.table_column_definition();
                tabl.addColumn(getColumn(column.column_name.getText(),
                        column.datatype, column.collate_name,
                        column.colmn_constraint, getDefSchemaName()));
            }
            if (tablAction.set_def_column() != null) {
                PgColumn col = tabl.getColumn(QNameParser.getFirstName(tablAction.column.identifier()));
                if (col != null) {
                    ValueExpr vex = new ValueExpr(schema.getName());
                    vex.analyze(new Vex(tablAction.set_def_column().expression));
                    col.addAllDeps(vex.getDepcies());
                }
            }

            if(tablAction.set_attribute_option() != null){
                PgColumn col = tabl.getColumn(QNameParser.getFirstName(tablAction.column.identifier()));
                if(col != null){
                    for (Storage_parameter_optionContext option :
                        tablAction.set_attribute_option().storage_parameter().storage_parameter_option()){
                        String value = option.value == null ? "" : option.value.getText();
                        fillOptionParams(value, option.storage_param.getText(), false, col::addOption);
                    }
                }
            }

            if (tablAction.define_foreign_options() != null) {
                PgColumn col = tabl.getColumn(QNameParser.getFirstName(tablAction.column.identifier()));
                if (col != null) {
                    for (Foreign_optionContext option : tablAction.define_foreign_options().foreign_option()) {
                        String value = option.value == null ? "" : option.value.getText();
                        fillOptionParams(value, option.name.getText(), false, col::addForeignOption);
                    }
                }
            }

            if (tablAction.set_storage() != null){
                PgColumn col = tabl.getColumn(QNameParser.getFirstName(tablAction.column.identifier()));
                if(col != null){
                    col.setStorage(tablAction.set_storage().storage_option().getText());
                }
            }

            if (tablAction.tabl_constraint != null) {
                PgConstraint constr = getTableConstraint(tablAction.tabl_constraint, schema.getName());
                if (tablAction.not_valid != null) {
                    constr.setNotValid(true);
                }
                tabl.addConstraint(constr);
            }
            if (tablAction.index_name != null) {
                IdentifierContext indexName = QNameParser.getFirstNameCtx(tablAction.index_name.identifier());
                PgIndex index = getSafe(tabl::getIndex, indexName);
                index.setClusterIndex(true);
            }

            if (tablAction.WITHOUT() != null && tablAction.OIDS() != null) {
                tabl.setHasOids(false);
            } else if (tablAction.WITH() != null && tablAction.OIDS() != null) {
                tabl.setHasOids(true);
            }

            if (tablAction.column != null) {
                if (tablAction.STATISTICS() != null) {
                    fillStatictics(tabl, tablAction);
                }
                if (tablAction.set_def_column() != null && tabl.getInherits().isEmpty()) {
                    // не добавляем в таблицу сиквенс если она наследует
                    // некоторые поля из др таблицы
                    // совместимость с текущей версией экспорта
                    fillDefColumn(tabl, tablAction);
                }
            }
            if (tablAction.RULE() != null) {
                createRule(tabl, tablAction);
            }



            if (tabl instanceof RegularPgTable) {
                RegularPgTable regTable = (RegularPgTable)tabl;

                // since 9.5 PostgreSQL
                if (tablAction.SECURITY() != null) {
                    if (tablAction.FORCE() != null) {
                        regTable.setForceSecurity(tablAction.NO() == null);
                    } else {
                        regTable.setRowSecurity(tablAction.ENABLE() != null);
                    }
                }
            }

            // since 10 PostgreSQL
            Identity_bodyContext identity = tablAction.identity_body();
            if (identity != null) {
                String name = null;
                for (Sequence_bodyContext body : identity.sequence_body()) {
                    if (body.NAME() != null) {
                        name = body.name.getText();
                    }
                }
                PgSequence sequence = new PgSequence(name, null);
                CreateSequence.fillSequence(sequence, identity.sequence_body());
                String columnName = QNameParser.getFirstName(tablAction.column.identifier());
                PgColumn column = tabl.getColumn(columnName);
                column.setSequence(sequence);
                column.setIdentityType(identity.ALWAYS() != null ? "ALWAYS" : "BY DEFAULT");
            }

        }
        return null;
    }

    private void createRule(PgTable tabl, Table_actionContext tablAction) {
        PgRule rule = getSafe(tabl::getRule, tablAction.rewrite_rule_name.identifier(0));
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

    private void fillDefColumn(PgTable table, Table_actionContext tablAction) {
        String name = QNameParser.getFirstName(tablAction.column.identifier());
        if (table.getColumn(name) == null) {
            PgColumn col = new PgColumn(name);
            col.setDefaultValue(getFullCtxText(tablAction.set_def_column().expression));
            table.addColumn(col);
        } else {
            table.getColumn(name).setDefaultValue(
                    getFullCtxText(tablAction.set_def_column().expression));
        }
    }

    private void fillStatictics(PgTable table, Table_actionContext tablAction) {
        String name = QNameParser.getFirstName(tablAction.column.identifier());
        if (table.getColumn(name) == null) {
            PgColumn col = new PgColumn(name);
            String number = tablAction.integer.getText();
            col.setStatistics(Integer.valueOf(number));
            table.addColumn(col);
        } else {
            table.getColumn(name).setStatistics(
                    Integer.valueOf(tablAction.integer.getText()));
        }
    }
}
