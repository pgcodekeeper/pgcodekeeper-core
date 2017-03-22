package cz.startnet.utils.pgdiff.parsers.antlr.statements;

import java.util.ArrayList;
import java.util.List;

import cz.startnet.utils.pgdiff.parsers.antlr.QNameParser;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Alter_table_statementContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.IdentifierContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Table_actionContext;
import cz.startnet.utils.pgdiff.parsers.antlr.expr.ValueExpr;
import cz.startnet.utils.pgdiff.parsers.antlr.rulectx.Vex;
import cz.startnet.utils.pgdiff.schema.GenericColumn;
import cz.startnet.utils.pgdiff.schema.PgColumn;
import cz.startnet.utils.pgdiff.schema.PgConstraint;
import cz.startnet.utils.pgdiff.schema.PgDatabase;
import cz.startnet.utils.pgdiff.schema.PgIndex;
import cz.startnet.utils.pgdiff.schema.PgRule;
import cz.startnet.utils.pgdiff.schema.PgStatement;
import cz.startnet.utils.pgdiff.schema.PgTable;
import ru.taximaxim.codekeeper.apgdiff.model.difftree.DbObjType;

public class AlterTable extends ParserAbstract {

    private final Alter_table_statementContext ctx;

    public AlterTable(Alter_table_statementContext ctx, PgDatabase db) {
        super(db);
        this.ctx = ctx;
    }

    @Override
    public PgStatement getObject() {
        List<IdentifierContext> ids = ctx.name.identifier();
        String name = QNameParser.getFirstName(ids);
        String schemaName = QNameParser.getSchemaName(ids, getDefSchemaName());
        PgTable tabl = db.getSchema(schemaName).getTable(name);

        List<String> sequences = new ArrayList<>();
        for (Table_actionContext tablAction : ctx.table_action()) {
            PgStatement st = null;
            if (tablAction.owner_to() != null) {
                if ((st = tabl) != null) {
                    fillOwnerTo(tablAction.owner_to(), st);
                } else if ((st = db.getSchema(schemaName).getSequence(name)) != null) {
                    fillOwnerTo(tablAction.owner_to(), st);
                } else if ((st = db.getSchema(schemaName).getView(name)) != null) {
                    fillOwnerTo(tablAction.owner_to(), st);
                }
            }
            if (tabl == null) {
                continue;
            }
            if (tablAction.table_column_definition() != null) {
                tabl.addColumn(getColumn(tablAction.table_column_definition(),
                        sequences, getDefSchemaName()));
            }
            if (tablAction.set_def_column() != null) {
                String sequence = getSequence(tablAction.set_def_column().expression);
                if (sequence != null) {
                    sequences.add(sequence);
                }
                PgColumn col = tabl.getColumn(QNameParser.getFirstName(tablAction.column.identifier()));
                if (col != null) {
                    ValueExpr vex = new ValueExpr(schemaName);
                    vex.analyze(new Vex(tablAction.set_def_column().expression));
                    col.addAllDeps(vex.getDepcies());
                }
            }
            if (tablAction.tabl_constraint != null) {
                PgConstraint constr = getTableConstraint(tablAction.tabl_constraint, schemaName, name);
                if (tablAction.not_valid != null) {
                    constr.setNotValid(true);
                }
                tabl.addConstraint(constr);
            }
            if (tablAction.index_name != null) {
                String indexName = QNameParser.getFirstName(tablAction.index_name.identifier());
                PgIndex index = tabl.getIndex(indexName);
                if (index == null) {
                    logError(indexName, schemaName);
                } else {
                    index.setClusterIndex(true);
                }
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
                if (tablAction.set_def_column() != null) {
                    // не добавляем в таблицу сиквенс если она наследует
                    // некоторые поля из др таблицы
                    // совместимость с текущей версией экспорта
                    if (tabl.getInherits().isEmpty()) {
                        fillDefColumn(tabl, tablAction);
                    }
                }
            }
            if (tablAction.RULE() != null) {
                PgRule rule = tabl.getRule(tablAction.rewrite_rule_name.getText());
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
        }
        for (String seq : sequences) {
            // не добавляем в таблицу сиквенс если она наследует некоторые поля
            // из др таблицы
            // совместимость с текущей версией экспорта
            if (tabl.getInherits().isEmpty()) {
                QNameParser seqName = new QNameParser(seq);
                tabl.addDep(new GenericColumn(seqName.getSchemaName(getDefSchemaName()),
                        seqName.getFirstName(), DbObjType.SEQUENCE));
            }
        }
        return null;
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
