package cz.startnet.utils.pgdiff.parsers.antlr.expr;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.antlr.v4.runtime.ParserRuleContext;

import cz.startnet.utils.pgdiff.PgDiffUtils;
import cz.startnet.utils.pgdiff.parsers.antlr.AntlrParser;
import cz.startnet.utils.pgdiff.parsers.antlr.QNameParser;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Alias_clauseContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Array_bracketsContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Array_expressionContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Case_expressionContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Cast_specificationContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Comparison_modContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Data_typeContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Datetime_overlapsContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Extract_functionContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Filter_clauseContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Frame_boundContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Frame_clauseContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.From_itemContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.From_primaryContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Function_args_parserContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Function_callContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Function_nameContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.General_literalContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Groupby_clauseContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Grouping_elementContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Grouping_set_listContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.IdentifierContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Indirection_identifierContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Orderby_clauseContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Ordinary_grouping_setContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Partition_by_columnsContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Predefined_typeContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Row_value_predicand_listContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Schema_qualified_nameContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Schema_qualified_name_nontypeContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Select_primaryContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Select_stmtContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Select_stmt_no_parensContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Sort_specifierContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.String_value_functionContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Table_subqueryContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Type_coercionContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Unsigned_value_specificationContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Value_expression_primaryContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Values_stmtContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Values_valuesContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.VexContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Vex_bContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Window_definitionContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.With_clauseContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.With_queryContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Xml_functionContext;
import cz.startnet.utils.pgdiff.parsers.antlr.rulectx.SelectOps;
import cz.startnet.utils.pgdiff.parsers.antlr.rulectx.SelectStmt;
import cz.startnet.utils.pgdiff.parsers.antlr.rulectx.Vex;
import cz.startnet.utils.pgdiff.parsers.antlr.statements.ParserAbstract;
import cz.startnet.utils.pgdiff.schema.GenericColumn;
import ru.taximaxim.codekeeper.apgdiff.Log;
import ru.taximaxim.codekeeper.apgdiff.model.difftree.DbObjType;

public class ViewSelect {

    private final String schema;
    private final ViewSelect parent;
    private final Set<GenericColumn> depcies;

    /**
     * CTE names that current level of FROM has access to.
     */
    private final Set<String> cte = new HashSet<>();


    public ViewSelect(String schema) {
        this.schema = schema;
        parent = null;
        depcies = new LinkedHashSet<>();
    }

    private ViewSelect(ViewSelect parent) {
        this.schema = parent.schema;
        this.parent = parent;
        depcies = parent.depcies;
    }

    public void analyze(SelectStmt select) {
        With_clauseContext with = select.withClause();
        if (with != null) {
            analyzeCte(with);
        }

        selectOps(select.selectOps());

        Orderby_clauseContext orderBy = select.orderBy();
        List<VexContext> vexs = null;
        if (select.limit() != null || select.offset() != null || select.fetch() != null) {
            vexs = select.vex();
        }

        if (orderBy != null || vexs != null) {
            if (orderBy != null) {
                orderBy(orderBy);
            }

            if (vexs != null) {
                vexs.forEach(v -> analyze(new Vex(v)));
            }
        }

        if (select.of(0) != null) {
            for (Schema_qualified_nameContext tableLock : select.schemaQualifiedName()) {
                addObjectDepcy(tableLock.identifier(), DbObjType.TABLE);
            }
        }
    }

    public Set<GenericColumn> getDepcies() {
        return Collections.unmodifiableSet(depcies);
    }

    private ViewSelect findCte(String cteName) {
        if (cte.contains(cteName)) {
            return this;
        }

        return parent == null ? null : parent.findCte(cteName);
    }

    private void analyze(ParserRuleContext ruleCtx) {
        if (ruleCtx instanceof Select_stmtContext) {
            analyze(new SelectStmt((Select_stmtContext) ruleCtx));
        } else if (ruleCtx instanceof Select_stmt_no_parensContext) {
            analyze(new SelectStmt((Select_stmt_no_parensContext) ruleCtx));
        } else {
            throw new IllegalStateException("Not a select ctx");
        }
    }

    private void addNameReference(Schema_qualified_nameContext name) {
        List<IdentifierContext> ids = name.identifier();
        String firstName = QNameParser.getFirstName(ids);

        boolean isCte = ids.size() == 1 && findCte(firstName) != null;
        if (!isCte) {
            addObjectDepcy(ids, DbObjType.TABLE);
        }
    }

    private void analyzeCte(With_clauseContext with) {
        boolean recursive = with.RECURSIVE() != null;
        for (With_queryContext withQuery : with.with_query()) {
            String withName = withQuery.query_name.getText();

            Select_stmtContext withSelect = withQuery.select_stmt();
            if (withSelect == null) {
                Log.log(Log.LOG_WARNING, "Skipped analisys of modifying CTE " + withName);
                continue;
            }

            // add CTE name to the visible CTEs list after processing the query for normal CTEs
            // and before for recursive ones
            ViewSelect withProcessor = new ViewSelect(this);
            boolean duplicate;
            if (recursive) {
                duplicate = !cte.add(withName);
                withProcessor.analyze(withSelect);
            } else {
                withProcessor.analyze(withSelect);
                duplicate = !cte.add(withName);
            }
            if (duplicate) {
                Log.log(Log.LOG_WARNING, "Duplicate CTE " + withName);
            }
        }
    }

    private void selectOps(SelectOps selectOps) {
        Select_stmtContext selectStmt = selectOps.selectStmt();
        Select_primaryContext primary;

        if (selectOps.leftParen() != null && selectOps.rightParen() != null && selectStmt != null) {
            analyze(selectStmt);
        } else if (selectOps.intersect() != null || selectOps.union() != null || selectOps.except() != null) {
            // analyze each in a separate scope
            // use column names from the first one
            new ViewSelect(this).selectOps(selectOps.selectOps(0));
            new ViewSelect(this).selectOps(selectOps.selectOps(1));
        } else if ((primary = selectOps.selectPrimary()) != null) {
            Values_stmtContext values;

            if (primary.SELECT() != null) {
                // from defines the namespace so it goes before everything else
                if (primary.FROM() != null) {
                    primary.from_item().forEach(this::from);
                }

                primary.select_list().select_sublist().forEach(t -> analyze(new Vex(t.vex())));

                if ((primary.set_qualifier() != null && primary.ON() != null)
                        || primary.WHERE() != null || primary.HAVING() != null) {
                    primary.vex().forEach(v -> analyze(new Vex(v)));
                }

                Groupby_clauseContext groupBy = primary.groupby_clause();
                if (groupBy != null) {
                    for (Grouping_elementContext group : groupBy.grouping_element_list().grouping_element()) {
                        Ordinary_grouping_setContext groupingSet = group.ordinary_grouping_set();
                        Grouping_set_listContext groupingSets;

                        if (groupingSet != null) {
                            groupingSet(groupingSet);
                        } else if ((groupingSets = group.grouping_set_list()) != null) {
                            groupingSets.ordinary_grouping_set_list().ordinary_grouping_set().forEach(this::groupingSet);
                        }
                    }
                }

                if (primary.WINDOW() != null) {
                    for (Window_definitionContext window : primary.window_definition()) {
                        window(window);
                    }
                }
            } else if (primary.TABLE() != null) {
                addObjectDepcy(primary.schema_qualified_name().identifier(), DbObjType.TABLE);
            } else if ((values = primary.values_stmt()) != null) {
                for (Values_valuesContext vals : values.values_values()) {
                    vals.vex().forEach(v -> analyze(new Vex(v)));
                }
            } else {
                Log.log(Log.LOG_WARNING, "No alternative in select_primary!");
            }
        } else {
            Log.log(Log.LOG_WARNING, "No alternative in SelectOps!");
        }
    }

    private void groupingSet(Ordinary_grouping_setContext groupingSet) {
        VexContext v = groupingSet.vex();
        Row_value_predicand_listContext predicandList;
        if (v != null) {
            analyze(new Vex(v));
        } else if ((predicandList = groupingSet.row_value_predicand_list()) != null) {
            predicandList.vex().forEach(p -> analyze(new Vex(p)));
        }
    }

    private void from(From_itemContext fromItem) {
        From_primaryContext primary;

        if (fromItem.LEFT_PAREN() != null && fromItem.RIGHT_PAREN() != null) {
            Alias_clauseContext joinAlias = fromItem.alias_clause();
            if (joinAlias != null) {
                // we simplify this case by analyzing joined ranges in an isolated scope
                // this way we get dependencies and don't pollute this scope with names hidden by the join alias
                // the only name this form of FROM clause exposes is the join alias

                // consequence of this method: no way to connect column references with the tables inside the join
                // that would require analyzing the table schemas and actually "performing" the join
                ViewSelect fromProcessor = new ViewSelect(this);
                fromProcessor.from(fromItem.from_item(0));
            } else {
                from(fromItem.from_item(0));
            }
        } else if (fromItem.JOIN() != null) {
            from(fromItem.from_item(0));
            from(fromItem.from_item(1));

            if (fromItem.ON() != null) {
                VexContext joinOn = fromItem.vex();
                analyze(new Vex(joinOn));
            }
        } else if ((primary = fromItem.from_primary()) != null) {
            Schema_qualified_nameContext table = primary.schema_qualified_name();
            Table_subqueryContext subquery;
            Function_callContext function;

            if (table != null) {
                addNameReference(table);
            } else if ((subquery = primary.table_subquery()) != null) {
                new ViewSelect(this).analyze(subquery.select_stmt());
            } else if ((function = primary.function_call()) != null) {
                function(function);
            } else {
                Log.log(Log.LOG_WARNING, "No alternative in from_primary!");
            }
        } else {
            Log.log(Log.LOG_WARNING, "No alternative in from_item!");
        }
    }

    private GenericColumn addObjectDepcy(List<IdentifierContext> ids, DbObjType type) {
        GenericColumn depcy = new GenericColumn(
                QNameParser.getSchemaName(ids, schema), QNameParser.getFirstName(ids), type);
        depcies.add(depcy);
        return depcy;
    }

    private GenericColumn addFunctionDepcy(Schema_qualified_name_nontypeContext funcNameCtx){
        IdentifierContext sch = funcNameCtx.schema;
        String funcSchema = sch != null ? sch.getText() : schema;
        String funcName = funcNameCtx.identifier_nontype().getText();
        GenericColumn depcy = new GenericColumn(funcSchema, funcName, DbObjType.FUNCTION);
        depcies.add(depcy);
        return depcy;
    }

    private void addTypeDepcy(Data_typeContext type) {
        Schema_qualified_name_nontypeContext typeName = type.predefined_type().schema_qualified_name_nontype();

        if (typeName != null) {
            IdentifierContext qual = typeName.identifier();
            String schema = qual == null ? this.schema : qual.getText();

            depcies.add(new GenericColumn(schema,
                    typeName.identifier_nontype().getText(), DbObjType.TYPE));
        }
    }

    private void addFunctionSigDepcy(String signature) {
        SQLParser p = AntlrParser.makeBasicParser(SQLParser.class, signature, "function signature");
        Function_args_parserContext sig = p.function_args_parser();
        List<IdentifierContext> ids = sig.schema_qualified_name().identifier();
        depcies.add(new GenericColumn(QNameParser.getSchemaName(ids, schema),
                PgDiffUtils.getQuotedName(QNameParser.getFirstName(ids)) +
                ParserAbstract.getFullCtxText(sig.function_args()),
                DbObjType.FUNCTION));
    }

    private void addSchemaDepcy(List<IdentifierContext> ids) {
        depcies.add(new GenericColumn(QNameParser.getFirstName(ids), DbObjType.SCHEMA));
    }

    private void regCast(String s, String regcast) {
        DbObjType regcastType;
        switch (regcast) {
        case "regproc":
            regcastType = DbObjType.FUNCTION;
            break;
        case "regclass":
            regcastType = DbObjType.TABLE;
            break;
        case "regtype":
            regcastType = DbObjType.TYPE;
            break;

        case "regnamespace":
            addSchemaDepcy(new QNameParser(s).getIds());
            return;

        case "regprocedure":
            addFunctionSigDepcy(s);
            return;

        case "regoper":
        case "regoperator":
            // TODO pending DbObjType.OPERATOR
        default:
            return;
        }

        addObjectDepcy(new QNameParser(s).getIds(), regcastType);
    }

    private void analyze(Vex vex) {
        Data_typeContext dataType = vex.dataType();
        // TODO OpCtx user-operator reference
        Select_stmt_no_parensContext selectStmt;
        Datetime_overlapsContext overlaps;
        Value_expression_primaryContext primary;
        boolean doneWork = true;

        if (vex.castExpression() != null && dataType != null) {
            addTypeDepcy(dataType);

            Predefined_typeContext pType = dataType.predefined_type();
            Schema_qualified_name_nontypeContext customType = pType.schema_qualified_name_nontype();
            IdentifierContext typeSchema = customType == null ? null : customType.identifier();
            // TODO remove when tokens are refactored
            if (dataType.LEFT_BRACKET() == null && dataType.SETOF() == null && customType != null &&
                    (typeSchema == null || "pg_catalog".equals(typeSchema.getText()))) {
                // check simple built-in types for reg*** casts
                Value_expression_primaryContext castPrimary = vex.vex().get(0).primary();
                Unsigned_value_specificationContext value;
                General_literalContext literal;
                if (castPrimary != null
                        && (value = castPrimary.unsigned_value_specification()) != null
                        && (literal = value.general_literal()) != null
                        && literal.character_string() != null) {
                    regCast(PgDiffUtils.unquoteQuotedString(literal.getText()),
                            customType.getText());
                }
            }
        } else if (vex.in() != null && vex.leftParen() != null && vex.rightParen() != null &&
                (selectStmt = vex.selectStmt()) != null) {
            new ViewSelect(this).analyze(selectStmt);
        } else if ((overlaps = vex.datetimeOverlaps()) != null) {
            overlaps.vex().forEach(v -> analyze(new Vex(v)));
        } else if ((primary = vex.primary()) != null) {
            Select_stmt_no_parensContext subSelectStmt = primary.select_stmt_no_parens();
            Case_expressionContext caseExpr;
            Cast_specificationContext cast;
            Comparison_modContext compMod;
            Table_subqueryContext subquery;
            Function_callContext function;
            Indirection_identifierContext indirection;
            Array_expressionContext array;
            Type_coercionContext typeCoercion;
            List<Vex> subOperands = null;

            if (primary.LEFT_PAREN() != null && primary.RIGHT_PAREN() != null &&
                    subSelectStmt != null) {
                new ViewSelect(this).analyze(subSelectStmt);
            } else if ((caseExpr = primary.case_expression()) != null) {
                subOperands = addVexCtxtoList(subOperands, caseExpr.vex());
            } else if ((cast = primary.cast_specification()) != null) {
                analyze(new Vex(cast.vex()));
                addTypeDepcy(cast.data_type());
            } else if ((compMod = primary.comparison_mod()) != null) {
                VexContext compModVex = compMod.vex();
                if (compModVex != null) {
                    analyze(new Vex(compModVex));
                } else {
                    new ViewSelect(this).analyze(compMod.select_stmt_no_parens());
                }
            } else if (primary.EXISTS() != null &&
                    (subquery = primary.table_subquery()) != null) {
                new ViewSelect(this).analyze(subquery.select_stmt());
            } else if ((function = primary.function_call()) != null) {
                function(function);
            } else if ((indirection = primary.indirection_identifier()) != null) {
                analyze(new Vex(indirection.vex()));
            } else if ((array = primary.array_expression()) != null) {
                Array_bracketsContext arrayb = array.array_brackets();
                if (arrayb != null) {
                    subOperands = addVexCtxtoList(subOperands, arrayb.vex());
                } else {
                    new ViewSelect(this).analyze(array.array_query().table_subquery().select_stmt());
                }
            } else if ((typeCoercion = primary.type_coercion()) != null) {
                addTypeDepcy(typeCoercion.data_type());
            }

            if (subOperands != null) {
                for (Vex v : subOperands) {
                    analyze(v);
                }
            }
        } else {
            doneWork = false;
        }

        List<Vex> operands = vex.vex();
        if (!operands.isEmpty()) {
            operands.forEach(this::analyze);
        } else if (!doneWork) {
            Log.log(Log.LOG_WARNING, "No alternative in Vex!");
        }
    }

    private void window(Window_definitionContext window) {
        Partition_by_columnsContext partition = window.partition_by_columns();
        if (partition != null) {
            for (VexContext v : partition.vex()) {
                analyze(new Vex(v));
            }
        }

        Orderby_clauseContext orderBy = window.orderby_clause();
        if (orderBy != null) {
            orderBy(orderBy);
        }

        Frame_clauseContext frame = window.frame_clause();
        if (frame != null) {
            for (Frame_boundContext bound : frame.frame_bound()) {
                VexContext vex = bound.vex();
                if (vex != null) {
                    analyze(new Vex(vex));
                }
            }
        }
    }

    private void orderBy(Orderby_clauseContext orderBy) {
        for (Sort_specifierContext sort : orderBy.sort_specifier_list().sort_specifier()) {
            analyze(new Vex(sort.vex()));
        }
    }

    /**
     * @return function reference or null for internal functions
     */
    private GenericColumn function(Function_callContext function) {
        GenericColumn ret = null;
        List<Vex> args = null;

        Function_nameContext name = function.function_name();

        Extract_functionContext extract;
        String_value_functionContext string;
        Xml_functionContext xml;

        if (name != null){
            Data_typeContext type = name.data_type();
            Schema_qualified_name_nontypeContext funcNameCtx;
            if (type != null &&
                    (funcNameCtx = type.predefined_type().schema_qualified_name_nontype()) != null) {
                ret = addFunctionDepcy(funcNameCtx);

            }

            args = addVexCtxtoList(args, function.vex());

            Orderby_clauseContext orderBy = function.orderby_clause();
            if (orderBy != null) {
                orderBy(orderBy);
            }
            Filter_clauseContext filter = function.filter_clause();
            if (filter != null) {
                analyze(new Vex(filter.vex()));
            }
            Window_definitionContext window = function.window_definition();
            if (window != null) {
                window(window);
            }
        } else if ((extract = function.extract_function()) != null) {
            analyze(new Vex(extract.vex()));
        } else if ((string = function.string_value_function()) != null) {
            args = addVexCtxtoList(args, string.vex());

            Vex_bContext vexB = string.vex_b();
            if (vexB != null) {
                analyze(new Vex(vexB));
            }
        } else if ((xml = function.xml_function()) != null) {
            args = addVexCtxtoList(args, xml.vex());
        }

        if (args != null) {
            for (Vex v : args) {
                analyze(v);
            }
        }
        return ret;
    }

    private List<Vex> addVexCtxtoList(List<Vex> l, List<VexContext> ctx) {
        int toAdd = ctx.size();
        if (toAdd != 0) {
            if (l == null) {
                l = new ArrayList<>(toAdd);
            }
            for (VexContext vexCtx : ctx) {
                l.add(new Vex(vexCtx));
            }
        }
        return l;
    }
}
