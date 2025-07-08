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
package org.pgcodekeeper.core.parsers.antlr.statements.ch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.QNameParser;
import org.pgcodekeeper.core.parsers.antlr.expr.launcher.ChExpressionAnalysisLauncher;
import org.pgcodekeeper.core.parsers.antlr.generated.CHParser.Data_type_exprContext;
import org.pgcodekeeper.core.parsers.antlr.generated.CHParser.Engine_clauseContext;
import org.pgcodekeeper.core.parsers.antlr.generated.CHParser.Engine_optionContext;
import org.pgcodekeeper.core.parsers.antlr.generated.CHParser.ExprContext;
import org.pgcodekeeper.core.parsers.antlr.generated.CHParser.Qualified_nameContext;
import org.pgcodekeeper.core.parsers.antlr.generated.CHParser.Subquery_clauseContext;
import org.pgcodekeeper.core.parsers.antlr.generated.CHParser.Table_column_defContext;
import org.pgcodekeeper.core.parsers.antlr.generated.CHParser.Table_constraint_defContext;
import org.pgcodekeeper.core.parsers.antlr.generated.CHParser.Table_index_defContext;
import org.pgcodekeeper.core.parsers.antlr.generated.CHParser.UsersContext;
import org.pgcodekeeper.core.parsers.antlr.statements.ParserAbstract;
import org.pgcodekeeper.core.schema.AbstractColumn;
import org.pgcodekeeper.core.schema.AbstractSchema;
import org.pgcodekeeper.core.schema.GenericColumn;
import org.pgcodekeeper.core.schema.PgObjLocation;
import org.pgcodekeeper.core.schema.PgStatement;
import org.pgcodekeeper.core.schema.PgObjLocation.LocationType;
import org.pgcodekeeper.core.schema.ch.ChColumn;
import org.pgcodekeeper.core.schema.ch.ChConstraint;
import org.pgcodekeeper.core.schema.ch.ChDatabase;
import org.pgcodekeeper.core.schema.ch.ChEngine;
import org.pgcodekeeper.core.schema.ch.ChIndex;
import org.pgcodekeeper.core.schema.ch.ChSchema;
import org.pgcodekeeper.core.settings.ISettings;

public abstract class ChParserAbstract extends ParserAbstract<ChDatabase> {

    protected ChParserAbstract(ChDatabase db, ISettings settings) {
        super(db, settings);
    }

    public static List<ParserRuleContext> getIdentifiers(Qualified_nameContext qNameCtx) {
        List<ParserRuleContext> ids = new ArrayList<>(3);
        ids.addAll(qNameCtx.identifier());
        return ids;
    }

    protected ChColumn getColumn(Table_column_defContext column) {
        List<ParserRuleContext> ids = getIdentifiers(column.qualified_name());
        var col = new ChColumn(QNameParser.getFirstName(ids));
        Data_type_exprContext typeExpr = column.data_type_expr();
        if (typeExpr.data_type() != null) {
            col.setType(getFullCtxText(typeExpr.data_type()));
            col.setNullValue(!isColumnNotNullable(typeExpr));

            var defType = typeExpr.table_column_property_expr();
            if (defType != null) {
                if (defType.DEFAULT() != null) {
                    col.setDefaultType("DEFAULT");
                } else if (defType.MATERIALIZED() != null) {
                    col.setDefaultType("MATERIALIZED");
                } else if (defType.ALIAS() != null) {
                    col.setDefaultType("ALIAS");
                } else if (defType.EPHEMERAL() != null) {
                    col.setDefaultType("EPHEMERAL");
                }
                if (defType.expr() != null) {
                    setExprWithAnalyze(AbstractColumn::setDefaultValue, col, defType.expr());
                }
            }
        }
        if (column.not_null() != null) {
            col.setNullValue(column.not_null().NOT() != null);
        }
        if (column.comment_expr() != null) {
            col.setComment(column.comment_expr().STRING_LITERAL().getText());
        }
        if (column.codec_expr() != null) {
            for (var codec : column.codec_expr().codec_arg_expr()) {
                col.addCodec(getFullCtxText(codec));
            }
        }
        if (column.TTL() != null) {
            setExprWithAnalyze(ChColumn::setTtl, col, column.ttl);
        }
        return col;
    }

    private boolean isColumnNotNullable(Data_type_exprContext typeExpr) {
        return (typeExpr.not_null() != null && typeExpr.not_null().NOT() != null)
                || typeExpr.data_type().NULLABLE() == null;
    }

    protected String getQuery(Subquery_clauseContext vQuery) {
        return getFullCtxText(vQuery);
    }

    protected ChEngine getEnginePart(Engine_clauseContext engineClause) {
        if (engineClause == null) {
            return null;
        }

        var engineCtx = engineClause.engine_expr();
        ChEngine engine = new ChEngine(engineCtx.NULL() != null ? "Null" : getFullCtxText(engineCtx.identifier()));
        if (engineCtx.expr_list() != null) {
            engine.setBody(getFullCtxText(engineCtx.expr_list()));
        }
        for (var option : engineClause.engine_option()) {
            parseEngineOption(engine, option);
        }
        if (Objects.equals(engine.getName(), "MergeTree") && !engine.containsOption("index_granularity")) {
            engine.addOption("index_granularity", "8192");
        }

        return engine;
    }

    protected void parseEngineOption(ChEngine engine, Engine_optionContext optionCtx) {
        var orderBy = optionCtx.order_by_clause();
        if (orderBy != null) {
            var orderByList = orderBy.order_expr_list();
            engine.setOrderBy(orderByList != null ? getFullCtxText(orderByList) : "()");
            return;
        }

        var pk = optionCtx.primary_key_clause();
        if (pk != null) {
            engine.setPrimaryKey(getFullCtxText(pk.expr()));
            return;
        }

        var partBy = optionCtx.partition_by_clause();
        if (partBy != null) {
            engine.setPartitionBy(getFullCtxText(partBy.expr()));
            return;
        }

        var ttl = optionCtx.ttl_clause();
        if (ttl != null) {
            engine.setTtl(getFullCtxText(ttl.ttl_expr_list()));
            return;
        }

        var settings = optionCtx.settings_clause();
        if (settings != null) {
            for (var setting : settings.pairs().pair()) {
                engine.addOption(setting.identifier().getText(), getFullCtxText(setting.expr()));
            }
            return;
        }

        var sampleBy = optionCtx.sample_by_clause();
        if (sampleBy != null) {
            engine.setSampleBy(getFullCtxText(sampleBy.expr()));
        }
    }

    protected ChConstraint getConstraint(Table_constraint_defContext constraintCtx) {
        var constr = new ChConstraint(constraintCtx.identifier().getText(), constraintCtx.ASSUME() != null);
        setExprWithAnalyze(ChConstraint::setExpr, constr, constraintCtx.expr());
        return constr;
    }

    protected ChIndex getIndex(Table_index_defContext indexCtx) {
        var index = new ChIndex(indexCtx.identifier().getText());
        setExprWithAnalyze(ChIndex::setExpr, index, indexCtx.expr());
        index.setType(getFullCtxText(indexCtx.index_type()));
        var granVal = indexCtx.gran;
        if (granVal != null) {
            index.setGranVal(Integer.parseInt(granVal.getText()));
        }
        return index;
    }

    private <T extends PgStatement> void setExprWithAnalyze(BiConsumer<T, String> adder, T stmt,
            ExprContext ctx) {
        adder.accept(stmt, getFullCtxText(ctx));
        db.addAnalysisLauncher(new ChExpressionAnalysisLauncher(stmt, ctx, fileName));
    }

    @Override
    protected PgObjLocation getLocation(List<? extends ParserRuleContext> ids, DbObjType type, String action,
            boolean isDep, String signature, LocationType locationType) {
        ParserRuleContext nameCtx = QNameParser.getFirstNameCtx(ids);

        if (type == DbObjType.FUNCTION) {
            return buildLocation(nameCtx, action, locationType, new GenericColumn(nameCtx.getText(), type));
        }

        if (type == DbObjType.POLICY) {
            String shortName = nameCtx.getText();
            String tableName = getFullCtxText(QNameParser.getSchemaNameCtx(ids));
            String fullName = shortName + " ON " + tableName;
            return buildLocation(nameCtx, action, locationType, new GenericColumn(fullName, type));
        }

        return super.getLocation(ids, type, action, isDep, signature, locationType);
    }

    protected <T extends PgStatement> void addRoles(UsersContext usersCtx, T stmt,
            BiConsumer<T, String> addRoleMethod, BiConsumer<T, String> addExceptMethod, String ignoreRole) {
        if (usersCtx == null) {
            return;
        }

        for (var roleCtx : usersCtx.roles.identifier()) {
            String role = roleCtx.getText();
            addDepSafe(stmt, Arrays.asList(roleCtx), DbObjType.ROLE);
            if (!ignoreRole.equalsIgnoreCase(role)) {
                addRoleMethod.accept(stmt, role);
            }

        }

        var exceptRolesCtx = usersCtx.excepts;
        if (exceptRolesCtx != null) {
            for (var exceptCtx : exceptRolesCtx.identifier()) {
                addExceptMethod.accept(stmt, exceptCtx.getText());
                addDepSafe(stmt, Arrays.asList(exceptCtx), DbObjType.ROLE);
            }
        }
    }

    @Override
    protected AbstractSchema createSchema(String name) {
        return new ChSchema(name);
    }
}
