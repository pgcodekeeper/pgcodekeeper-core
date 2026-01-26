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

import java.util.*;
import java.util.Map.Entry;
import java.util.function.*;
import java.util.stream.Stream;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.parser.*;
import org.pgcodekeeper.core.database.base.parser.antlr.AbstractExpr;
import org.pgcodekeeper.core.database.base.schema.meta.*;
import org.pgcodekeeper.core.database.pg.PgDiffUtils;
import org.pgcodekeeper.core.database.pg.parser.PgParserUtils;
import org.pgcodekeeper.core.database.pg.parser.generated.SQLParser.*;
import org.pgcodekeeper.core.database.pg.parser.statement.PgParserAbstract;
import org.pgcodekeeper.core.loader.FullAnalyze;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.utils.*;

/**
 * Abstract base class for PostgreSQL expression analysis.
 * Provides core functionality for dependency tracking and reference resolution.
 */
public abstract class PgAbstractExpr extends AbstractExpr {

    protected static final String NONAME = "?column?";

    private FullAnalyze fullAnalyze;

    protected PgAbstractExpr(MetaContainer meta) {
        super(meta);
    }

    protected PgAbstractExpr(PgAbstractExpr parent) {
        super(parent);
        this.fullAnalyze = parent.fullAnalyze;
    }

    protected PgAbstractExpr(PgAbstractExpr parent, Set<ObjectLocation> dependencies) {
        super(parent, dependencies, parent.meta);
        this.fullAnalyze = parent.fullAnalyze;
    }

    public void setFullAnalyze(FullAnalyze fullAnalyze) {
        this.fullAnalyze = fullAnalyze;
    }

    /**
     * @param name alias of the referenced object
     * @return a pair of (Alias, ColumnsList) where Alias is the given name.
     * ColumnsList list of columns as pair 'columnName-columnType' of the internal query.<br>
     */
    protected List<Pair<String, String>> findReferenceComplex(String name) {
        if (parent instanceof PgAbstractExpr pgAbstractExpr) {
            return pgAbstractExpr.findReferenceComplex(name);
        }

        return null;
    }

    protected Pair<String, String> findColumnInComplex(String name) {
        if (parent instanceof PgAbstractExpr pgAbstractExpr) {
            return pgAbstractExpr.findColumnInComplex(name);
        }
        return null;
    }

    protected GenericColumn addRelationDepcy(List<ParserRuleContext> ids) {
        return addDepcy(ids, DbObjType.TABLE, null);
    }

    protected GenericColumn addDepcy(List<ParserRuleContext> ids, DbObjType type, Token start) {
        ParserRuleContext schemaCtx = QNameParser.getSchemaNameCtx(ids);
        ParserRuleContext nameCtx = QNameParser.getFirstNameCtx(ids);
        String name = nameCtx.getText();

        if (schemaCtx == null) {
            return new GenericColumn(Consts.PG_CATALOG, name, type);
        }
        String schemaName = schemaCtx.getText();
        GenericColumn depcy = new GenericColumn(schemaName, name, type);
        addDepcy(new GenericColumn(schemaName, DbObjType.SCHEMA), schemaCtx, start);
        addDepcy(depcy, nameCtx, start);
        return depcy;
    }

    protected GenericColumn addTypeDepcy(Data_typeContext type) {
        Schema_qualified_name_nontypeContext typeName = type.predefined_type().schema_qualified_name_nontype();
        if (typeName == null) {
            return new GenericColumn(Consts.PG_CATALOG, PgParserAbstract.getTypeName(type),
                    DbObjType.TYPE);
        }
        return addTypeDepcy(typeName);
    }

    protected GenericColumn addTypeDepcy(Schema_qualified_name_nontypeContext typeName) {
        return addDepcy(PgParserAbstract.getIdentifiers(typeName), DbObjType.TYPE, null);
    }

    protected void addDepcy(GenericColumn depcy, ParserRuleContext ctx, Token start) {
        if (!isSystemSchema(depcy.schema())) {
            ObjectLocation loc = new ObjectLocation.Builder()
                    .setObject(depcy)
                    .setCtx(ctx)
                    .build();
            if (start instanceof CodeUnitToken codeUnitStart) {
                loc = loc.copyWithOffset(codeUnitStart.getCodeUnitStart(),
                        codeUnitStart.getLine() - 1, codeUnitStart.getCodeUnitPositionInLine(), null);
            }

            addDependency(loc);
        }
    }

    @Override
    protected boolean isSystemSchema(String schema) {
        return PgDiffUtils.isSystemSchema(schema);
    }

    protected void addDepcy(ObjectLocation loc) {
        if (!isSystemSchema(loc.getSchema())) {
            addDependency(loc);
        }
    }

    /**
     * @return column with its type
     */
    protected ModPair<String, String> processColumn(List<? extends ParserRuleContext> ids) {
        if (ids.size() == 1) {
            return processTablelessColumn(ids.get(0));
        }

        String columnName = QNameParser.getFirstName(ids);
        ParserRuleContext columnParentCtx = QNameParser.getSecondNameCtx(ids);
        String columnParent = columnParentCtx.getText();
        ParserRuleContext schemaNameCtx = QNameParser.getThirdNameCtx(ids);
        String schemaName = schemaNameCtx == null ? null : schemaNameCtx.getText();

        String columnType = IPgTypesSetManually.COLUMN;
        Entry<String, GenericColumn> ref = findReference(schemaName, columnParent, columnName);
        List<Pair<String, String>> refComplex;
        if (ref != null) {
            GenericColumn referencedTable = ref.getValue();
            if (referencedTable != null) {
                if (schemaNameCtx != null) {
                    addDependency(new GenericColumn(referencedTable.schema(), DbObjType.SCHEMA), schemaNameCtx);
                }

                if (referencedTable.getObjName().equals(columnParent)) {
                    addDependency(referencedTable, columnParentCtx);
                } else {
                    addReference(referencedTable, columnParentCtx);
                }

                columnType = addFilteredColumnDepcy(
                        referencedTable.schema(), referencedTable.table(), columnName);
            } else if ((refComplex = findReferenceComplex(columnParent)) != null) {
                columnType = refComplex.stream()
                        .filter(entry -> columnName.equals(entry.getFirst()))
                        .map(Pair::getSecond)
                        .findAny()
                        .orElseGet(() -> {
                            log(columnParentCtx, Messages.AbstractExpr_log_column_not_found_in_complex, columnName, columnParent);
                            return IPgTypesSetManually.COLUMN;
                        });
            } else {
                log(columnParentCtx, Messages.AbstractExpr_log_complex_not_found, columnParent);
            }
        } else {
            log(columnParentCtx, Messages.AbstractExpr_log_unknown_column_ref, schemaName, columnParent, columnName);
        }

        return new ModPair<>(columnName, columnType);
    }

    /**
     * Add a dependency only from the column of the user object. Always return its type.
     *
     * @param schemaName   object schema
     * @param relationName user or system object which contains column 'colName'
     * @param colName      dependency from this column will be added
     * @return column type
     */
    protected String addFilteredColumnDepcy(String schemaName, String relationName, String colName) {
        Stream<Pair<String, String>> columns = addFilteredRelationColumnsDepcies(
                schemaName, relationName, col -> col.equals(colName));
        // handle system columns; look for relation anyway for a potential 'not found' warning
        // do not use the stream nor add the depcy though
        return switch (colName) {
            case "oid", "tableoid" -> "oid";
            case "xmin", "xmax" -> "xid";
            case "cmin", "cmax" -> "cid";
            case "ctid" -> "tid";
            default -> columns.findAny().map(Pair::getSecond).orElseGet(() -> {
                log(Messages.AbstractExpr_log_column_not_found_in_relation, colName, relationName);
                return IPgTypesSetManually.COLUMN;
            });
        };

    }

    /**
     * Terminal operation must be called on the returned stream for depcy addition to take effect! <br>
     * <br>
     * Returns a stream of relation columns filtered with the given predicate. When this stream is terminated, and if
     * the relation is a user relation, side-effect depcy-addition is performed for all columns satisfying the
     * predicate. <br>
     * If a short-circuiting operation is used to terminate the stream then only some column depcies will be added. <br>
     * <br>
     * This ugly solution was chosen because all others lead to any of the following:<br>
     * <ul>
     * <li>code duplicaton</li>
     * <li>depcy addition/relation search logic leaking into other classes</li>
     * <li>inefficient filtering on the hot path (predicate matching a single column)</li>
     * <li>and/or other performance/allocation inefficiencies</li>
     * </ul>
     *
     * @return column stream with attached depcy-addition peek-step; empty stream if no relation found
     */
    protected Stream<Pair<String, String>> addFilteredRelationColumnsDepcies(String schemaName,
                                                                             String relationName,
                                                                             Predicate<String> colNamePredicate) {
        IRelation relation = findRelation(schemaName, relationName);
        if (relation == null) {
            log(Messages.AbstractExpr_log_relation_not_found, schemaName, relationName);
            return Stream.empty();
        }

        Stream<Pair<String, String>> columns = relation.getRelationColumns();
        if (DbObjType.VIEW == relation.getStatementType() && columns == null) {
            analyzeViewColumns(relation);
            columns = relation.getRelationColumns();

            if (columns == null) {
                return Stream.empty();
            }
        }

        Stream<Pair<String, String>> cols = columns
                .filter(col -> colNamePredicate.test(col.getFirst()));

        String relSchemaName = relation.getSchemaName();
        if (isSystemSchema(relSchemaName)) {
            return cols;
        }

        // hack
        return cols.peek(col -> addDependency(new GenericColumn(relSchemaName,
                relation.getName(), col.getFirst(), DbObjType.COLUMN), null));
    }

    protected void analyzeViewColumns(IRelation rel) {
        if (fullAnalyze != null) {
            fullAnalyze.analyzeView(rel);
        }
    }

    protected ModPair<String, String> processTablelessColumn(ParserRuleContext id) {
        String name = id.getText();
        Pair<String, String> col = findColumnInComplex(name);
        if (col == null) {
            Pair<IRelation, Pair<String, String>> relCol = findColumn(name);
            if (relCol == null) {
                log(id, Messages.AbstractExpr_log_tableless_column_not_resolved, name);
                return new ModPair<>(name, IPgTypesSetManually.COLUMN);
            }
            IRelation rel = relCol.getFirst();
            col = relCol.getSecond();
            addDependency(new GenericColumn(rel.getSchemaName(), rel.getName(),
                    col.getFirst(), DbObjType.COLUMN), id);
        }
        return col.copyMod();
    }

    protected void addColumnsDepcies(Schema_qualified_nameContext table,
                                     List<Indirection_identifierContext> columns) {
        List<ParserRuleContext> ids = PgParserAbstract.getIdentifiers(table);
        String schemaName = QNameParser.getSchemaName(ids);
        String tableName = QNameParser.getFirstName(ids);
        for (Indirection_identifierContext col : columns) {
            // only column name
            addFilteredColumnDepcy(schemaName, tableName, col.identifier().getText());
        }
    }

    protected void addFunctionDepcy(IFunction function, ParserRuleContext ctx) {
        addDependency(new GenericColumn(function.getSchemaName(), function.getName(),
                function.getStatementType()), ctx);
    }

    /**
     * Use only in contexts where function can be pinpointed only by its name.
     * Such as ::regproc casts.
     */

    protected void addFunctionDepcyNotOverloaded(List<ParserRuleContext> ids, Token start, DbObjType type) {
        ParserRuleContext schemaCtx = QNameParser.getSchemaNameCtx(ids);
        if (schemaCtx == null) {
            return;
        }

        String schemaName = schemaCtx.getText();
        if (isSystemSchema(schemaName)) {
            return;
        }
        addDepcy(new GenericColumn(schemaName, DbObjType.SCHEMA), schemaCtx, start);

        ParserRuleContext nameCtx = QNameParser.getFirstNameCtx(ids);
        String name = nameCtx.getText();

        Collection<? extends IStatement> availableStatements =
                type == DbObjType.OPERATOR ? availableOperators(schemaName) : availableFunctions(schemaName);

        for (IStatement statement : availableStatements) {
            if (statement.getBareName().equals(name)) {
                addDepcy(new GenericColumn(schemaName, statement.getName(), type), nameCtx, start);
                break;
            }
        }
    }

    protected void addFunctionSigDepcy(String signature, Token start, DbObjType type) {
        var parser = PgParserUtils.createSqlParser(signature, "signature parser", null, start);

        List<ParserRuleContext> ids;
        UnaryOperator<String> fullNameOperator;
        if (type == DbObjType.OPERATOR) {
            Operator_args_parserContext sig = parser.operator_args_parser();
            ids = PgParserAbstract.getIdentifiers(sig.operator_name());
            fullNameOperator = name -> PgParserAbstract.parseOperatorSignature(name, sig.operator_args());
        } else {
            Function_args_parserContext sig = parser.function_args_parser();
            ids = PgParserAbstract.getIdentifiers(sig.schema_qualified_name());
            fullNameOperator = name -> PgParserAbstract.parseSignature(name, sig.function_args());
        }

        ParserRuleContext schemaCtx = QNameParser.getSchemaNameCtx(ids);
        if (schemaCtx == null) {
            return;
        }

        String schemaName = schemaCtx.getText();
        if (isSystemSchema(schemaName)) {
            return;
        }
        addDepcy(new GenericColumn(schemaName, DbObjType.SCHEMA), schemaCtx, start);

        ParserRuleContext nameCtx = QNameParser.getFirstNameCtx(ids);
        String name = nameCtx.getText();
        String fullName = fullNameOperator.apply(name);
        addDepcy(new GenericColumn(schemaName, fullName, type), nameCtx, start);
    }

    protected void addSchemaDepcy(List<ParserRuleContext> ids, Token start) {
        ParserRuleContext ctx = QNameParser.getFirstNameCtx(ids);
        addDepcy(new GenericColumn(ctx.getText(), DbObjType.SCHEMA), ctx, start);
    }

    protected Collection<IFunction> availableFunctions(String schemaName) {
        return meta.availableFunctions(getSchemaName(schemaName));
    }

    protected Collection<IOperator> availableOperators(String schemaName) {
        return meta.availableOperators(getSchemaName(schemaName));
    }

    protected MetaCompositeType findType(String schemaName, String typeName) {
        return meta.findType(getSchemaName(schemaName), typeName);
    }

    @Override
    protected String getSchemaName(String schemaName) {
        if (schemaName == null) {
            return Consts.PG_CATALOG;
        }
        return super.getSchemaName(schemaName);
    }
}
