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
package org.pgcodekeeper.core.database.pg.parser.expr;

import java.util.Map.Entry;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.pgcodekeeper.core.database.api.schema.GenericColumn;
import org.pgcodekeeper.core.database.base.schema.meta.MetaContainer;
import org.pgcodekeeper.core.database.pg.parser.generated.SQLParser.*;
import org.pgcodekeeper.core.database.pg.parser.rulectx.*;
import org.pgcodekeeper.core.database.pg.parser.statement.PgParserAbstract;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.utils.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Parser for PL/pgSQL function definitions with namespace support.
 */
public final class PgFunctionExp extends PgAbstractExprWithNmspc<Plpgsql_functionContext> {

    /**
     * Creates a Function parser with meta container.
     *
     * @param meta the meta container with schema information
     */
    public PgFunctionExp(MetaContainer meta) {
        super(meta);
    }

    private PgFunctionExp(PgAbstractExpr parent) {
        super(parent);
    }

    @Override
    public List<ModPair<String, String>> analyze(Plpgsql_functionContext root) {
        block(root.function_block());
        return Collections.emptyList();
    }

    private void block(Function_blockContext block) {
        // block label may be used to qualify references to vars in nested blocks
        // there's no mechanism to track this ATM, implement later if requested
        // same for loop-declared vars (at least with the FOR var IN ... syntax)
        declare(block.declarations());
        statements(block.function_statements());
        exception(block.exception_statement());
    }

    private void declare(DeclarationsContext declare) {
        if (declare == null) {
            return;
        }

        PgValueExpr vex = new PgValueExpr(this);

        for (DeclarationContext declaration : declare.declaration()) {
            String alias = declaration.identifier().getText();
            Type_declarationContext dec = declaration.type_declaration();

            Data_type_decContext datatype = dec.data_type_dec();
            if (datatype != null) {
                declareVar(alias, datatype);

                VexContext vexCtx = dec.vex();
                if (vexCtx != null) {
                    vex.analyze(new PgVex(vexCtx));
                }
            } else if (dec.ALIAS() != null) {
                ParseTree idVar = dec.identifier();
                if (idVar == null) {
                    idVar = dec.DOLLAR_NUMBER();
                }
                String variable = idVar.getText();

                declareAlias(alias, variable);
            } else if (dec.CURSOR() != null) {
                Arguments_listContext list = dec.arguments_list();
                if (list != null) {
                    for (Data_typeContext type : list.data_type()) {
                        addTypeDepcy(type);
                    }
                }

                new PgSelect(this).analyze(new PgSelectStmt(dec.select_stmt()));
                addNamespaceVariable(new Pair<>(alias, IPgTypesSetManually.CURSOR));
            }
        }
    }

    private void declareVar(String alias, Data_type_decContext ctx) {
        Data_typeContext type = ctx.data_type();
        if (type != null) {
            declareNamespaceVar(alias, null, addTypeDepcy(type));
        } else if (ctx.ROWTYPE() != null) {
            declareNamespaceVar(alias, null, addTypeDepcy(ctx.schema_qualified_name_nontype()));
        } else {
            List<? extends ParserRuleContext> ids;
            if (ctx.dollar_number() != null) {
                ids = Collections.singletonList(ctx.dollar_number());
            } else {
                ids = PgParserAbstract.getIdentifiers(ctx.schema_qualified_name());
            }

            String varType = processColumn(ids).getSecond();
            addNamespaceVariable(new Pair<>(alias, varType));
        }
    }

    private void declareAlias(String alias, String variable) {
        Entry<String, GenericColumn> ref = findReference(null, variable, null);
        if (ref != null) {
            addReference(alias, ref.getValue());
        } else {
            Pair<String, String> pair = findColumnInComplex(variable);
            String type;
            if (pair != null) {
                type = pair.getSecond();
            } else {
                type = IPgTypesSetManually.UNKNOWN;
                log(Messages.Function_log_variable_not_found, variable);
            }

            addNamespaceVariable(new Pair<>(alias, type));
        }
    }

    private void statements(Function_statementsContext statements) {
        for (Function_statementContext statement : statements.function_statement()) {
            Function_blockContext block = statement.function_block();
            Base_statementContext base;
            Control_statementContext control;
            Cursor_statementContext cursor;
            Message_statementContext message;
            Plpgsql_queryContext query;
            Transaction_statementContext transaction;
            Additional_statementContext additional;
            if (block != null) {
                new PgFunctionExp(this).block(block);
            } else if ((base = statement.base_statement()) != null) {
                base(base);
            } else if ((control = statement.control_statement()) != null) {
                control(control);
            } else if ((cursor = statement.cursor_statement()) != null) {
                cursor(cursor);
            } else if ((message = statement.message_statement()) != null) {
                message(message);
            } else if ((query = statement.plpgsql_query()) != null) {
                query(query);
            } else if ((transaction = statement.transaction_statement()) != null) {
                transaction(transaction);
            } else if ((additional = statement.additional_statement()) != null) {
                additional(additional);
            }
        }
    }

    private void base(Base_statementContext base) {
        Assign_stmtContext assign = base.assign_stmt();
        Perform_stmtContext perform;

        if (assign != null) {
            Select_stmt_no_parensContext select = assign.select_stmt_no_parens();
            if (select != null) {
                new PgSelect(this).analyze(new PgSelectStmt(select));
            } else {
                new PgSelect(this).analyze(assign.perform_stmt());
            }
        } else if ((perform = base.perform_stmt()) != null) {
            new PgSelect(this).analyze(perform);
        }
    }

    private void execute(Execute_stmtContext exec) {
        PgValueExpr vex = new PgValueExpr(this);
        vex.analyze(new PgVex(exec.vex()));
        Using_vexContext using = exec.using_vex();
        if (using != null) {
            for (VexContext v : using.vex()) {
                vex.analyze(new PgVex(v));
            }
        }
    }

    private void control(Control_statementContext control) {
        PgValueExpr vex = new PgValueExpr(this);

        Return_stmtContext returnStmt = control.return_stmt();
        If_statementContext isc;
        Case_statementContext csc;
        Loop_statementContext loop;

        if (returnStmt != null) {
            returnStmt(returnStmt);
        } else if (control.CALL() != null) {
            vex.function(control.function_call());
        } else if ((isc = control.if_statement()) != null) {
            for (VexContext vexCtx : isc.vex()) {
                vex.analyze(new PgVex(vexCtx));
            }
            for (Function_statementsContext statements : isc.function_statements()) {
                statements(statements);
            }
        } else if ((csc = control.case_statement()) != null) {
            for (VexContext vexCtx : csc.vex()) {
                vex.analyze(new PgVex(vexCtx));
            }
            for (Function_statementsContext statements : csc.function_statements()) {
                statements(statements);
            }
        } else if ((loop = control.loop_statement()) != null) {
            loop(loop);
        }
    }

    private void loop(Loop_statementContext loop) {
        Function_statementsContext statements = loop.function_statements();
        VexContext vexCtx;

        if (statements != null) {
            PgFunctionExp function = new PgFunctionExp(this);
            Loop_startContext start = loop.loop_start();
            if (start != null) {
                function.loopStart(start);
            }
            function.statements(statements);
        } else if ((vexCtx = loop.vex()) != null) {
            PgValueExpr vex = new PgValueExpr(this);
            vex.analyze(new PgVex(vexCtx));
        }
    }

    private void loopStart(Loop_startContext start) {
        IdentifierContext cur = start.cursor;
        if (cur != null) {
            // record
            addNamespaceVariable(new Pair<>(cur.getText(), IPgTypesSetManually.UNKNOWN));
        } else if (start.DOUBLE_DOT() != null) {
            addNamespaceVariable(new Pair<>(start.alias.getText(), IPgTypesSetManually.INTEGER));
        }

        List<VexContext> vexs = start.vex();
        List<OptionContext> options = start.option();
        if (!vexs.isEmpty() || !options.isEmpty()) {
            PgValueExpr vex = new PgValueExpr(this);

            for (VexContext v : vexs) {
                vex.analyze(new PgVex(v));
            }

            for (OptionContext option : options) {
                vex.analyze(new PgVex(option.vex()));
            }
        }

        Plpgsql_queryContext query = start.plpgsql_query();
        if (query != null) {
            var columns = query(query);
            if (columns.isEmpty()) {
                return;
            }
            var rec = start.identifier_list().identifier();
            if (rec.size() != 1) {
                return;
            }
            var key = rec.get(0).getText();
            addReference(key, null);
            complexNamespace.put(key, new ArrayList<>(columns));
        }
    }

    private void returnStmt(Return_stmtContext returnStmt) {
        VexContext vexCtx = returnStmt.vex();
        Plpgsql_queryContext query;
        Perform_stmtContext perform;

        if (vexCtx != null) {
            PgValueExpr vex = new PgValueExpr(this);
            vex.analyze(new PgVex(vexCtx));
        } else if ((query = returnStmt.plpgsql_query()) != null) {
            query(query);
        } else if ((perform = returnStmt.perform_stmt()) != null) {
            new PgSelect(this).analyze(perform);
        }
    }

    private void cursor(Cursor_statementContext cursor) {
        List<OptionContext> options = cursor.option();
        Plpgsql_queryContext query;

        if (!options.isEmpty()) {
            PgValueExpr vex = new PgValueExpr(this);
            for (OptionContext option : options) {
                vex.analyze(new PgVex(option.vex()));
            }
        } else if ((query = cursor.plpgsql_query()) != null) {
            query(query);
        }
    }

    private void message(Message_statementContext message) {
        PgValueExpr vex = new PgValueExpr(this);
        for (VexContext vexCtx : message.vex()) {
            vex.analyze(new PgVex(vexCtx));
        }

        Raise_usingContext using = message.raise_using();
        if (using != null) {
            for (VexContext vexCtx : using.vex()) {
                vex.analyze(new PgVex(vexCtx));
            }
        }
    }

    private List<ModPair<String, String>> query(Plpgsql_queryContext query) {
        Data_statementContext ds = query.data_statement();
        Explain_statementContext statement;
        Execute_stmtContext exec;

        if (ds != null) {
            return new PgSql(this).data(ds);
        }
        if ((exec = query.execute_stmt()) != null) {
            execute(exec);
        } else if ((statement = query.explain_statement()) != null) {
            Explain_queryContext explain = statement.explain_query();
            ds = explain.data_statement();
            Execute_statementContext ex;
            Declare_statementContext dec;

            if (ds != null) {
                new PgSql(this).data(ds);
            } else if ((ex = explain.execute_statement()) != null) {
                PgValueExpr vex = new PgValueExpr(this);
                for (VexContext v : ex.vex()) {
                    vex.analyze(new PgVex(v));
                }
            } else if ((dec = explain.declare_statement()) != null) {
                new PgSelect(this).analyze(dec.select_stmt());
            }
        }
        return Collections.emptyList();
    }

    private void transaction(Transaction_statementContext transaction) {
        Lock_tableContext lock = transaction.lock_table();
        if (lock != null) {
            for (Only_table_multiplyContext name : lock.only_table_multiply()) {
                addRelationDepcy(PgParserAbstract.getIdentifiers(name.schema_qualified_name()));
            }
        }
    }

    private void additional(Additional_statementContext additional) {
        Schema_qualified_nameContext table = additional.schema_qualified_name();
        Data_statementContext data;
        Table_cols_listContext col;
        Truncate_stmtContext truncate;
        Reindex_stmtContext reindex;

        if ((reindex = additional.reindex_stmt()) != null) {
            if (reindex.TABLE() != null) {
                addRelationDepcy(PgParserAbstract.getIdentifiers(reindex.schema_qualified_name()));
            } else if (reindex.SCHEMA() != null) {
                addSchemaDepcy(PgParserAbstract.getIdentifiers(reindex.schema_qualified_name()), null);
            }
        } else if (table != null && additional.REFRESH() != null) {
            addRelationDepcy(PgParserAbstract.getIdentifiers(table));
        } else if ((data = additional.data_statement()) != null) {
            new PgSql(this).data(data);

            for (Data_typeContext type : additional.data_type()) {
                addTypeDepcy(type);
            }
        } else if ((col = additional.table_cols_list()) != null) {
            for (Inheritance_specified_table_colsContext tabl : col.inheritance_specified_table_cols()) {
                List<ParserRuleContext> ids = PgParserAbstract.getIdentifiers(tabl.schema_qualified_name());
                GenericColumn rel = addRelationDepcy(ids);
                var columns = tabl.columns();
                if (columns != null) {
                    for (IdentifierContext id : columns.identifier()) {
                        addFilteredColumnDepcy(rel.schema(), rel.table(), id.getText());
                    }
                }
            }
        } else if ((truncate = additional.truncate_stmt()) != null) {
            for (Only_table_multiplyContext name : truncate.only_table_multiply()) {
                addRelationDepcy(PgParserAbstract.getIdentifiers(name.schema_qualified_name()));
            }
        }
    }

    private void exception(Exception_statementContext exception) {
        if (exception == null) {
            return;
        }

        PgValueExpr vex = new PgValueExpr(this);
        for (VexContext vexCtx : exception.vex()) {
            vex.analyze(new PgVex(vexCtx));
        }

        for (Function_statementsContext statements : exception.function_statements()) {
            statements(statements);
        }
    }
}
