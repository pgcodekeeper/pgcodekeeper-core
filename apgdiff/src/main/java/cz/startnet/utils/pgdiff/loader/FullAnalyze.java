package cz.startnet.utils.pgdiff.loader;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.antlr.v4.runtime.ParserRuleContext;
import org.jgrapht.event.TraversalListenerAdapter;
import org.jgrapht.event.VertexTraversalEvent;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.TopologicalOrderIterator;

import cz.startnet.utils.pgdiff.parsers.antlr.AntlrError;
import cz.startnet.utils.pgdiff.parsers.antlr.CustomParserListener;
import cz.startnet.utils.pgdiff.parsers.antlr.CustomSQLParserListener;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Create_rewrite_statementContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Data_typeContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Delete_stmt_for_psqlContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Function_argumentsContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.IdentifierContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Identifier_nontypeContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Index_restContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Insert_stmt_for_psqlContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Rewrite_commandContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Schema_qualified_name_nontypeContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Select_stmtContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Sort_specifierContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.SqlContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Update_stmt_for_psqlContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.VexContext;
import cz.startnet.utils.pgdiff.parsers.antlr.exception.UnresolvedReferenceException;
import cz.startnet.utils.pgdiff.parsers.antlr.expr.AbstractExprWithNmspc;
import cz.startnet.utils.pgdiff.parsers.antlr.expr.Delete;
import cz.startnet.utils.pgdiff.parsers.antlr.expr.Insert;
import cz.startnet.utils.pgdiff.parsers.antlr.expr.Select;
import cz.startnet.utils.pgdiff.parsers.antlr.expr.Sql;
import cz.startnet.utils.pgdiff.parsers.antlr.expr.Update;
import cz.startnet.utils.pgdiff.parsers.antlr.expr.ValueExpr;
import cz.startnet.utils.pgdiff.parsers.antlr.expr.ValueExprWithNmspc;
import cz.startnet.utils.pgdiff.parsers.antlr.rulectx.Vex;
import cz.startnet.utils.pgdiff.parsers.antlr.statements.ParserAbstract;
import cz.startnet.utils.pgdiff.schema.AbstractFunction;
import cz.startnet.utils.pgdiff.schema.AbstractSchema;
import cz.startnet.utils.pgdiff.schema.AbstractView;
import cz.startnet.utils.pgdiff.schema.GenericColumn;
import cz.startnet.utils.pgdiff.schema.PgDatabase;
import cz.startnet.utils.pgdiff.schema.PgRule;
import cz.startnet.utils.pgdiff.schema.PgStatement;
import cz.startnet.utils.pgdiff.schema.PgStatementWithSearchPath;
import cz.startnet.utils.pgdiff.schema.PgTrigger;
import ru.taximaxim.codekeeper.apgdiff.ApgdiffUtils;
import ru.taximaxim.codekeeper.apgdiff.model.difftree.DbObjType;
import ru.taximaxim.codekeeper.apgdiff.model.graph.DepcyGraph;
import ru.taximaxim.codekeeper.apgdiff.utils.Pair;

public final class FullAnalyze {

    private final List<AntlrError> errors;
    private final PgDatabase db;

    public FullAnalyze(PgDatabase db, List<AntlrError> errors) {
        this.db = db;
        this.errors = errors;
    }

    public static void fullAnalyze(PgDatabase db, List<AntlrError> errors) {
        new FullAnalyze(db, errors).fullAnalyze();
    }

    public void fullAnalyze() {
        TopologicalOrderIterator<PgStatement, DefaultEdge> orderIterator =
                new TopologicalOrderIterator<>(new DepcyGraph(db).getReversedGraph());

        orderIterator.addTraversalListener(new AnalyzeTraversalListenerAdapter(db, errors));

        // 'VIEW' statements analysis.
        while (orderIterator.hasNext()) {
            orderIterator.next();
        }

        // Analysis of all statements except 'VIEW'.
        for (Entry<PgStatementWithSearchPath, ParserRuleContext> entry : db.getContextsForAnalyze()) {
            PgStatementWithSearchPath statement = entry.getKey();
            DbObjType statementType = statement.getStatementType();

            // Duplicated objects doesn't have parent, skip them
            if (DbObjType.VIEW == statementType || statement.getParent() == null) {
                continue;
            }

            ParserRuleContext ctx = entry.getValue();
            if (statement.getDatabase() != db) {
                // statement came from another DB object, probably a library
                // for proper depcy processing, find its twin in the final DB object
                statement = (PgStatementWithSearchPath) statement.getTwin(db);
            }
            String schemaName = statement.getContainingSchema().getName();

            try {
                switch (statementType) {
                case RULE:
                    analyzeRulesCreate((Create_rewrite_statementContext) ctx,
                            (PgRule) statement, schemaName, db);
                    break;
                case TRIGGER:
                    analyzeTriggersWhen((VexContext) ctx,
                            (PgTrigger) statement, schemaName, db);
                    break;
                case INDEX:
                    analyzeIndexRest((Index_restContext) ctx, statement,
                            schemaName, db);
                    break;
                case CONSTRAINT:
                    analyzeWithNmspc((VexContext) ctx,
                            statement, schemaName, statement.getParent().getName(), db);
                    break;
                case FUNCTION:
                case PROCEDURE:
                    if (ctx instanceof VexContext) {
                        analyze((VexContext) ctx, new ValueExpr(db), statement);
                    } else {
                        analyzeDefinition((SqlContext) ctx, new Sql(db),
                                (AbstractFunction) statement);
                    }
                    break;
                case DOMAIN:
                case COLUMN:
                    analyze((VexContext) ctx, new ValueExpr(db), statement);
                    break;
                default:
                    throw new IllegalStateException("The analyze for the case '"
                            + statementType + ' ' + statement
                            + "' is not defined!"); //$NON-NLS-1$
                }

            } catch (UnresolvedReferenceException ex) {
                unresolvRefExHandler(ex, errors, ctx, statement.getLocation().getFilePath());
            } catch (Exception ex) {
                addError(errors, CustomParserListener.handleParserContextException(
                        ex, statement.getLocation().getFilePath(), ctx));
            }
        }

        db.getContextsForAnalyze().clear();
    }

    private void analyzeRulesCreate(Create_rewrite_statementContext createRewriteCtx,
            PgRule rule, String schemaName, PgDatabase db) {
        analyzeRulesWhere(createRewriteCtx, rule, schemaName, db);
        for (Rewrite_commandContext cmd : createRewriteCtx.commands) {
            analyzeRulesCommand(cmd, rule, schemaName, db);
        }
    }

    private void analyzeRulesWhere(Create_rewrite_statementContext ctx, PgRule rule,
            String schemaName, PgDatabase db) {
        if (ctx.WHERE() != null) {
            ValueExprWithNmspc vex = new ValueExprWithNmspc(db);
            GenericColumn implicitTable = new GenericColumn(schemaName, rule.getParent().getName(), DbObjType.TABLE);
            vex.addReference("new", implicitTable);
            vex.addReference("old", implicitTable);
            analyze(ctx.vex(), vex, rule);
        }
    }

    private void analyzeRulesCommand(Rewrite_commandContext cmd, PgRule rule,
            String schemaName, PgDatabase db) {
        Select_stmtContext select;
        Insert_stmt_for_psqlContext insert;
        Delete_stmt_for_psqlContext delete;
        Update_stmt_for_psqlContext update;
        if ((select = cmd.select_stmt()) != null) {
            analyzeRule(select, new Select(db), rule, schemaName);
        } else if ((insert = cmd.insert_stmt_for_psql()) != null) {
            analyzeRule(insert, new Insert(db), rule, schemaName);
        } else if ((delete = cmd.delete_stmt_for_psql()) != null) {
            analyzeRule(delete, new Delete(db), rule, schemaName);
        } else if ((update = cmd.update_stmt_for_psql()) != null) {
            analyzeRule(update, new Update(db), rule, schemaName);
        }
    }

    private <T extends ParserRuleContext> void analyzeRule(
            T ctx, AbstractExprWithNmspc<T> analyzer, PgRule rule, String schemaName) {
        GenericColumn implicitTable = new GenericColumn(schemaName, rule.getParent().getName(), DbObjType.TABLE);
        analyzer.addReference("new", implicitTable);
        analyzer.addReference("old", implicitTable);
        analyze(ctx, analyzer, rule);
    }

    private void analyzeTriggersWhen(VexContext ctx, PgTrigger trigger,
            String schemaName, PgDatabase db) {
        ValueExprWithNmspc vex = new ValueExprWithNmspc(db);
        GenericColumn implicitTable = new GenericColumn(schemaName,
                trigger.getParent().getName(), DbObjType.TABLE);
        vex.addReference("new", implicitTable);
        vex.addReference("old", implicitTable);
        analyze(ctx, vex, trigger);
    }

    private void analyzeIndexRest(Index_restContext rest, PgStatement indexStmt,
            String schemaName, PgDatabase db) {
        String rawTableReference = indexStmt.getParent().getName();

        for (Sort_specifierContext sort_ctx : rest.index_sort().sort_specifier_list()
                .sort_specifier()) {
            analyzeWithNmspc(sort_ctx.key, indexStmt, schemaName,
                    rawTableReference, db);
        }

        if (rest.index_where() != null){
            analyzeWithNmspc(rest.index_where().vex(), indexStmt,
                    schemaName, rawTableReference, db);
        }
    }

    private <T extends ParserRuleContext> void analyze(
            T ctx, AbstractExprWithNmspc<T> analyzer, PgStatement pg) {
        analyzer.analyze(ctx);
        pg.addAllDeps(analyzer.getDepcies());
    }

    private void analyze(VexContext ctx, ValueExpr analyzer, PgStatement pg) {
        analyzer.analyze(new Vex(ctx));
        pg.addAllDeps(analyzer.getDepcies());
    }

    private void analyzeWithNmspc(VexContext ctx, PgStatement statement,
            String schemaName, String rawTableReference, PgDatabase db) {
        ValueExprWithNmspc valExptWithNmspc = new ValueExprWithNmspc(db);
        valExptWithNmspc.addRawTableReference(new GenericColumn(schemaName,
                rawTableReference, DbObjType.TABLE));
        analyze(ctx, valExptWithNmspc, statement);
    }

    private <T extends ParserRuleContext> void analyzeDefinition(T ctx,
            AbstractExprWithNmspc<T> analyzer, AbstractFunction st) {
        List<Pair<String, String>> simpleFuncArgs = new ArrayList<>();
        Map<String, GenericColumn> relFuncArgs = new LinkedHashMap<>();
        splitFuncArgs(db, st, relFuncArgs, simpleFuncArgs);
        analyzer.addFuncArgsToNmsp(relFuncArgs, simpleFuncArgs);

        analyzer.analyze(ctx);
        st.addAllDeps(analyzer.getDepcies());
    }

    /**
     * Splits function arguments into simple arguments and arguments with relations.
     */
    private void splitFuncArgs(PgDatabase db, AbstractFunction func,
            Map<String, GenericColumn> rels, List<Pair<String, String>> prims) {
        Entry<PgStatementWithSearchPath, List<Function_argumentsContext>> funcArgs = db
                .getFuncArgsCtxsForAnalyze().stream().filter(e -> func.equals(e.getKey()))
                .findAny().orElse(null);

        if (funcArgs == null) {
            return;
        }

        List<Function_argumentsContext> argCtxs = funcArgs.getValue();
        for (int i = 0; i < argCtxs.size(); i++) {
            String argDollarName = "$" + (i + 1);
            Identifier_nontypeContext argNameCtx = argCtxs.get(i).argname;

            // TODO if (argName != null) need to put this with argDollarName
            String argName = argNameCtx == null ? null :
                ParserAbstract.getFullCtxText(argCtxs.get(i).argname);


            Data_typeContext dataTypeCtx = argCtxs.get(i).data_type();
            Schema_qualified_name_nontypeContext typeQname = dataTypeCtx.predefined_type()
                    .schema_qualified_name_nontype();

            if (typeQname != null) {
                IdentifierContext schemaCtx = typeQname.identifier();

                if (schemaCtx != null) {
                    String schemaName = ParserAbstract.getFullCtxText(schemaCtx);
                    String typeNameOfObj = ParserAbstract.getFullCtxText(typeQname.identifier_nontype());

                    if (ApgdiffUtils.isPgSystemSchema(schemaName)) {
                        // put result to the 'simpleFuncArgs'
                        prims.add(new Pair<>(argDollarName, typeNameOfObj));
                    } else {
                        // check if it is TABLE OR VIEW OR TYPE then put it to the 'relFuncArgs'
                        DbObjType type = null;
                        AbstractSchema schema = db.getSchema(schemaName);
                        if (schema.getTable(typeNameOfObj) != null) {
                            type = DbObjType.TABLE;
                        } else if (schema.getView(typeNameOfObj) != null) {
                            type = DbObjType.VIEW;
                        } else if (schema.getType(typeNameOfObj) != null) {
                            type = DbObjType.TYPE;
                        } else {
                            // else put it to the 'simpleFuncArgs'
                            prims.add(new Pair<>(argDollarName, typeNameOfObj));
                        }

                        if (type != null) {
                            rels.put(argDollarName,
                                    new GenericColumn(schemaName, typeNameOfObj, type));
                        }
                    }
                }
            } else {
                // put it to the 'simpleFuncArgs'
                prims.add(new Pair<>(argDollarName,
                        ParserAbstract.getTypeName(dataTypeCtx)));
            }
        }
    }


    private class AnalyzeTraversalListenerAdapter extends TraversalListenerAdapter<PgStatement, DefaultEdge> {

        private final PgDatabase db;
        private final List<AntlrError> errors;

        AnalyzeTraversalListenerAdapter(PgDatabase db, List<AntlrError> errors) {
            this.db = db;
            this.errors = errors;
        }

        @Override
        public void vertexTraversed(VertexTraversalEvent<PgStatement> event) {
            PgStatement st = event.getVertex();
            if (DbObjType.VIEW != st.getStatementType()) {
                return;
            }
            if (st.getDatabase() != db) {
                // same as above, get the object from the final DB
                st = st.getTwin(db);
            }
            PgStatement stmt = st;
            db.getContextsForAnalyze().stream().filter(e -> stmt.equals(e.getKey()))
            .forEach(e -> {
                ParserRuleContext ctx = e.getValue();
                try {
                    analyzeViewCtx(ctx, (AbstractView) e.getKey(), db);
                } catch (UnresolvedReferenceException ex) {
                    unresolvRefExHandler(ex, errors, ctx, stmt.getLocation().getFilePath());
                } catch (Exception ex) {
                    addError(errors, CustomParserListener.handleParserContextException(
                            ex, stmt.getLocation().getFilePath(), ctx));
                }
            });
        }

        private void analyzeViewCtx(ParserRuleContext ctx, AbstractView view,
                PgDatabase db) {
            if (ctx instanceof Select_stmtContext) {
                Select select = new Select(db);
                view.addRelationColumns(select.analyze((Select_stmtContext) ctx));
                view.addAllDeps(select.getDepcies());
            } else {
                analyze((VexContext) ctx, new ValueExpr(db), view);
            }
        }
    }

    private void unresolvRefExHandler(UnresolvedReferenceException ex,
            List<AntlrError> errors, ParserRuleContext ctx, String location) {
        if (ex.getErrorToken() == null) {
            ex.setErrorToken(ctx.getStart());
        }
        addError(errors, CustomSQLParserListener.handleUnresolvedReference(ex, location));
    }

    private void addError(List<AntlrError> errors, AntlrError err) {
        if (errors != null) {
            errors.add(err);
        }
    }
}
