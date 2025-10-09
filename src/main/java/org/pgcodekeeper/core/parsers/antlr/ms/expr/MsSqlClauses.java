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
package org.pgcodekeeper.core.parsers.antlr.ms.expr;

import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.*;
import org.pgcodekeeper.core.schema.GenericColumn;
import org.pgcodekeeper.core.schema.meta.MetaContainer;

import java.util.Collections;
import java.util.List;

/**
 * Microsoft SQL clauses analyzer.
 * Processes various SQL statement clauses including DML, DDL, and control flow statements
 * to extract database object dependencies.
 */
public class MsSqlClauses extends MsAbstractExpr {

    protected MsSqlClauses(MsAbstractExpr parent) {
        super(parent);
    }

    /**
     * Creates a new Microsoft SQL clauses analyzer with the specified schema and metadata.
     *
     * @param schema the current schema context
     * @param meta   the metadata container for database schema information
     */
    public MsSqlClauses(String schema, MetaContainer meta) {
        super(schema, meta);
    }

    /**
     * Analyzes SQL clauses and extracts database object dependencies.
     *
     * @param sql the SQL clauses context to analyze
     * @return empty list as this analyzer focuses on dependency extraction rather than column names
     */
    public List<String> analyze(Sql_clausesContext sql) {
        for (St_clauseContext st : sql.st_clause()) {
            clause(st);
        }

        return Collections.emptyList();
    }

    private void clause(St_clauseContext st) {
        Dml_clauseContext dml = st.dml_clause();
        Ddl_clauseContext ddl;
        Cfl_statementContext cfl;
        Dbcc_clauseContext dbcc;
        Another_statementContext another;
        if (dml != null) {
            dml(dml);
        } else if ((ddl = st.ddl_clause()) != null) {
            ddl(ddl);
        } else if ((cfl = st.cfl_statement()) != null) {
            cfl(cfl);
        } else if ((dbcc = st.dbcc_clause()) != null) {
            dbcc(dbcc);
        } else if ((another = st.another_statement()) != null) {
            another(another);
        }

    }

    void dml(Dml_clauseContext dml) {
        Merge_statementContext ms = dml.merge_statement();
        Delete_statementContext ds;
        Insert_statementContext is;
        Select_statementContext ss;
        Update_statementContext us;

        if (ms != null) {
            new MsMerge(this).analyze(ms);
        } else if ((ds = dml.delete_statement()) != null) {
            new MsDelete(this).analyze(ds);
        } else if ((is = dml.insert_statement()) != null) {
            new MsInsert(this).analyze(is);
        } else if ((ss = dml.select_statement()) != null) {
            new MsSelect(this).analyze(ss);
        } else if ((us = dml.update_statement()) != null) {
            new MsUpdate(this).analyze(us);
        }
    }

    private void ddl(Ddl_clauseContext ddl) {
        Lock_tableContext lt = ddl.lock_table();
        Truncate_tableContext tt;
        Enable_disable_triggerContext edt;
        Update_statisticsContext us;

        if (lt != null) {
            addObjectDepcy(lt.qualified_name(), DbObjType.TABLE);
        } else if ((tt = ddl.truncate_table()) != null) {
            addObjectDepcy(tt.qualified_name(), DbObjType.TABLE);
        } else if ((edt = ddl.enable_disable_trigger()) != null) {
            enableDisableTrigger(edt);
        } else if ((us = ddl.update_statistics()) != null) {
            updateStatistics(us);
        }
    }

    private void enableDisableTrigger(Enable_disable_triggerContext edt) {
        Qualified_nameContext qualifiedName = edt.qualified_name();
        if (qualifiedName != null) {
            GenericColumn cont = addObjectDepcy(qualifiedName, DbObjType.TABLE);
            Names_referencesContext names = edt.names_references();
            if (names != null) {
                for (Qualified_nameContext trig : names.qualified_name()) {
                    IdContext nameCtx = trig.name;
                    addDependency(new GenericColumn(cont.schema, cont.table,
                            nameCtx.getText(), DbObjType.TRIGGER), nameCtx);
                }
            }
        }
    }

    private void updateStatistics(Update_statisticsContext us) {
        GenericColumn cont = addObjectDepcy(us.table, DbObjType.TABLE);
        Qualified_nameContext index = us.index;
        Names_referencesContext names;
        if (index != null) {
            IdContext nameCtx = index.name;
            addDependency(
                    new GenericColumn(cont.schema, cont.table, nameCtx.getText(), DbObjType.TRIGGER), nameCtx);
        } else if ((names = us.names_references()) != null) {
            for (Qualified_nameContext ind : names.qualified_name()) {
                IdContext nameCtx = ind.name;
                addDependency(
                        new GenericColumn(cont.schema, cont.table, nameCtx.getText(), DbObjType.TRIGGER), nameCtx);
            }
        }
    }

    private void cfl(Cfl_statementContext cfl) {
        Block_statementContext block = cfl.block_statement();
        If_statementContext is;
        Return_statementContext rs;
        Try_catch_statementContext tcs;
        Waitfor_statementContext wfs;
        While_statementContext ws;
        Print_statementContext ps;

        if (block != null) {
            analyze(block.sql_clauses());
        } else if ((is = cfl.if_statement()) != null) {
            new MsValueExpr(this).search(is.search_condition());

            for (St_clauseContext clause : is.st_clause()) {
                clause(clause);
            }
        } else if ((rs = cfl.return_statement()) != null) {
            ExpressionContext exp = rs.expression();
            if (exp != null) {
                new MsValueExpr(this).analyze(rs.expression());
            }
        } else if ((tcs = cfl.try_catch_statement()) != null) {
            for (Sql_clausesContext clauses : tcs.sql_clauses()) {
                analyze(clauses);
            }
        } else if ((wfs = cfl.waitfor_statement()) != null) {
            Waitfor_receiveContext rec = wfs.waitfor_receive();
            if (rec != null) {
                receive(rec.receive_statement());
            }
        } else if ((ws = cfl.while_statement()) != null) {
            new MsValueExpr(this).search(ws.search_condition());
            clause(ws.st_clause());
        } else if ((ps = cfl.print_statement()) != null) {
            ExpressionContext exp = ps.expression();
            if (exp != null) {
                new MsValueExpr(this).analyze(exp);
            }
        }
    }

    private void receive(Receive_statementContext receive) {
        Top_clauseContext top = receive.top_clause();
        if (top != null) {
            ExpressionContext exp = top.top_count().expression();
            if (exp != null) {
                new MsValueExpr(this).analyze(exp);
            }
        }

        for (Receive_column_specifierContext col : receive.receive_column_specifier()) {
            ExpressionContext exp = col.expression();
            if (exp != null) {
                new MsValueExpr(this).analyze(exp);
            }
        }

        if (receive.WHERE() != null) {
            new MsValueExpr(this).search(receive.search_condition());
        }
    }

    private void dbcc(Dbcc_clauseContext dbcc) {
        Expression_listContext list = dbcc.expression_list();
        if (list != null) {
            new MsValueExpr(this).expressionList(list);
        }
    }

    private void another(Another_statementContext another) {
        Declare_statementContext dec = another.declare_statement();
        Cursor_statementContext cursor;
        Receive_statementContext receive;
        Execute_statementContext exec;
        Set_statementContext set;

        if (dec != null) {
            declare(dec);
        } else if ((cursor = another.cursor_statement()) != null) {
            cursor(cursor);
        } else if ((receive = another.receive_statement()) != null) {
            receive(receive);
        } else if ((exec = another.execute_statement()) != null) {
            Execute_moduleContext em = exec.execute_module();
            Qualified_nameContext qname;
            if (em != null && (qname = em.qualified_name()) != null) {
                addObjectDepcy(qname, DbObjType.FUNCTION);
            }
        } else if ((set = another.set_statement()) != null) {
            set(set);
        }
    }

    private void set(Set_statementContext set) {
        ExpressionContext exp = set.expression();
        Cursor_commonContext cc;

        if (exp != null) {
            new MsValueExpr(this).analyze(exp);
        } else if ((cc = set.cursor_common()) != null) {
            new MsSelect(this).analyze(cc.select_statement());
        }
    }

    private void cursor(Cursor_statementContext cursor) {
        ExpressionContext exp = cursor.expression();
        Cursor_commonContext cc;
        if (exp != null) {
            new MsValueExpr(this).analyze(exp);
        } else if ((cc = cursor.cursor_common()) != null) {
            new MsSelect(this).analyze(cc.select_statement());
        }
    }

    private void declare(Declare_statementContext dec) {
        for (Declare_localContext local : dec.declare_local()) {
            Data_typeContext type = local.data_type();
            if (type != null) {
                addTypeDepcy(type);
            }
            ExpressionContext exp = local.expression();
            if (exp != null) {
                new MsValueExpr(this).analyze(exp);
            }
        }

        var elementsCtx = dec.table_elements();
        if (elementsCtx != null) {
            for (Table_elementContext elementCtx : elementsCtx.table_element()) {
                var colCtx = elementCtx.column_def();
                if (colCtx != null) {
                    var dt = colCtx.data_type();
                    if (dt != null) {
                        addTypeDepcy(dt);
                    }
                }
            }
        }
    }
}
