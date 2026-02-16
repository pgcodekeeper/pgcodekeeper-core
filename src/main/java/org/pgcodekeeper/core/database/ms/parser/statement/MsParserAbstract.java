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
package org.pgcodekeeper.core.database.ms.parser.statement;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.parser.QNameParser;
import org.pgcodekeeper.core.database.base.parser.statement.ParserAbstract;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.database.ms.parser.generated.TSQLParser.*;
import org.pgcodekeeper.core.database.ms.parser.launcher.MsExpressionAnalysisLauncher;
import org.pgcodekeeper.core.database.ms.project.MsWorkDirs;
import org.pgcodekeeper.core.database.ms.schema.*;
import org.pgcodekeeper.core.database.ms.utils.MsDiffUtils;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.utils.FileUtils;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Abstract base class for Microsoft SQL statement parsers.
 * Provides common functionality for parsing Microsoft SQL-specific database objects
 * including tables, columns, constraints, indexes, and various SQL Server features.
 */
public abstract class MsParserAbstract extends ParserAbstract<MsDatabase> {

    private static final String SIZE_ONE = " (1)";

    protected MsParserAbstract(MsDatabase db, ISettings settings) {
        super(db, settings);
    }

    protected MsConstraint getMsPKConstraint(String schema, String table, String conName,
                                                   Table_constraint_bodyContext body) {
        var constrPk = new MsConstraintPk(conName, body.PRIMARY() != null);
        var clusteredCtx = body.clustered();
        constrPk.setClustered(clusteredCtx != null && clusteredCtx.CLUSTERED() != null);
        var dataspaceCtx = body.dataspace();
        if (dataspaceCtx != null) {
            constrPk.setDataSpace(buildDataSpace(dataspaceCtx));
        }

        fillColumns(constrPk, body.column_name_list_with_order().column_with_order(), schema, table);
        var optionsCtx = body.index_options();
        if (optionsCtx != null) {
            for (Index_optionContext option : optionsCtx.index_option()) {
                constrPk.addOption(option.key.getText(), getFullCtxText(option.index_option_value()));
            }
        }

        return constrPk;
    }

    protected void parseIndex(Index_restContext rest, MsIndex index, String schema, String table) {
        fillColumns(index, rest.index_sort().column_name_list_with_order().column_with_order(), null, null);
        Index_includeContext include = rest.index_include();
        if (include != null) {
            for (IdContext col : include.name_list_in_brackets().id()) {
                index.addInclude(col.getText());
                index.addDependency(new ObjectReference(schema, table, col.getText(), DbObjType.COLUMN));
            }
        }
        parseIndexOptions(index, rest.index_where(), rest.index_options(), rest.dataspace());
    }

    protected void parseIndexOptions(MsIndex index, Index_whereContext wherePart,
                                     Index_optionsContext options, DataspaceContext space) {
        if (wherePart != null) {
            index.setWhere(getFullCtxText(wherePart.where));
        }
        if (options != null) {
            fillOptions(index, options.index_option());
        }
        if (space != null) {
            index.setTablespace(buildDataSpace(space));
        }
    }

    protected void fillOptions(IOptionContainer stmt, List<Index_optionContext> options) {
        for (Index_optionContext option : options) {
            String key = option.key.getText();
            String value = getFullCtxText(option.index_option_value());
            if (Objects.equals("SYSTEM_VERSIONING", key) && stmt instanceof MsTable table) {
                table.setSysVersioning(value);
                addHistTableDep(option.index_option_value().on_option(), stmt);
            } else {
                stmt.addOption(key, value);
            }
        }
    }

    protected void addHistTableDep(On_optionContext onOpt, IOptionContainer stmt) {
        if (onOpt == null || isRefMode()) {
            return;
        }

        for (var sysVer : onOpt.system_versioning_opt()) {
            var historyTable = sysVer.history_table_name;
            if (null != historyTable) {
                var histSchemaName = historyTable.schema.getText();
                var histTableName = historyTable.name.getText();
                stmt.addDependency(new ObjectReference(histSchemaName, histTableName, DbObjType.TABLE));
            }
        }
    }

    private void fillColumnConstraint(Column_optionContext option, MsColumn col, IStatementContainer stmt) {
        String constraintName = option.constraint != null ? option.constraint.getText() : null;
        var constraintCtx = option.column_constraint_body();
        var isTableConstr = stmt instanceof MsTable;
        if (constraintCtx.DEFAULT() != null) {
            col.setDefaultName(constraintName);
            ExpressionContext expCtx = constraintCtx.expression();
            col.setDefaultValue(getFullCtxTextWithCheckNewLines(expCtx));
            db.addAnalysisLauncher(
                    new MsExpressionAnalysisLauncher(!isTableConstr ? (MsType) stmt : col, expCtx, fileName));
        } else if (constraintCtx.PRIMARY() != null || constraintCtx.UNIQUE() != null) {
            if (constraintName == null && isTableConstr) {
                if (constraintCtx.PRIMARY() != null) {
                    constraintName = stmt.getName() + '_' + col.getName() + "_pkey";
                } else {
                    constraintName = stmt.getName() + '_' + col.getName() + "_key";
                }
            }

            var constrPk = new MsConstraintPk(constraintName, constraintCtx.PRIMARY() != null);
            var clusteredCtx = constraintCtx.clustered();
            constrPk.setClustered(clusteredCtx != null && clusteredCtx.CLUSTERED() != null);
            var dataspaceCtx = constraintCtx.dataspace();
            if (dataspaceCtx != null) {
                constrPk.setDataSpace(buildDataSpace(dataspaceCtx));
            }
            var optionsCtx = constraintCtx.index_options();
            if (optionsCtx != null) {
                fillOptions(constrPk, optionsCtx.index_option());
            }
            var columns = constraintCtx.column_name_list_with_order();
            if (columns == null) {
                constrPk.addColumn(new SimpleColumn(col.getName()));
            } else {
                fillColumns(constrPk, columns.column_with_order(), null, null);
            }

            stmt.addChild(constrPk);
        } else if (constraintCtx.REFERENCES() != null) {
            if (constraintName == null) {
                constraintName = stmt.getName() + '_' + col.getName() + "_fkey";
            }

            var constrFk = new MsConstraintFk(constraintName);
            constrFk.addColumn(col.getName());
            Qualified_nameContext ref = constraintCtx.qualified_name();
            List<IdContext> ids = Arrays.asList(ref.schema, ref.name);
            String fSchemaName = getSchemaNameSafe(ids);
            String fTableName = QNameParser.getFirstName(ids);
            ObjectLocation loc = addObjReference(ids, DbObjType.TABLE, null);
            ObjectReference fTable = loc.getObjectReference();
            constrFk.addDependency(fTable);
            constrFk.setForeignSchema(fSchemaName);
            constrFk.setForeignTable(fTableName);

            IdContext column = constraintCtx.id();
            if (column != null) {
                String colFk = column.getText();
                constrFk.addForeignColumn(colFk);
                constrFk.addDependency(new ObjectReference(fSchemaName, fTableName, colFk, DbObjType.COLUMN));
            }
            var del = constraintCtx.on_delete();
            if (del != null) {
                if (del.CASCADE() != null) {
                    constrFk.setDelAction("CASCADE");
                } else if (del.NULL() != null) {
                    constrFk.setDelAction("SET NULL");
                } else if (del.DEFAULT() != null) {
                    constrFk.setDelAction("SET DEFAULT");
                }
            }
            var upd = constraintCtx.on_update();
            if (upd != null) {
                if (upd.CASCADE() != null) {
                    constrFk.setUpdAction("CASCADE");
                } else if (upd.NULL() != null) {
                    constrFk.setUpdAction("SET NULL");
                } else if (upd.DEFAULT() != null) {
                    constrFk.setUpdAction("SET DEFAULT");
                }
            }
            if (constraintCtx.not_for_replication() != null) {
                constrFk.setNotForRepl(true);
            }

            stmt.addChild(constrFk);
        } else if (constraintCtx.CHECK() != null) {
            if (constraintName == null && isTableConstr) {
                constraintName = stmt.getName() + '_' + col.getName() + "_check";
            }

            var constrCheck = new MsConstraintCheck(constraintName);
            constrCheck.setNotForRepl(constraintCtx.not_for_replication() != null);
            constrCheck.setExpression(getFullCtxTextWithCheckNewLines(constraintCtx.search_condition()));

            stmt.addChild(constrCheck);
        }
    }

    protected void fillColumnOption(Column_optionContext option, MsColumn col, IStatementContainer stmt) {
        if (option.SPARSE() != null) {
            col.setSparse(true);
        } else if (option.COLLATE() != null) {
            col.setCollation(getFullCtxText(option.collate));
        } else if (option.GENERATED() != null) {
            col.setGenerated(getGenerated(option));
            col.setHidden(option.HIDDEN_KEYWORD() != null);
        } else if (option.PERSISTED() != null) {
            col.setPersisted(true);
        } else if (option.ROWGUIDCOL() != null) {
            col.setRowGuidCol(true);
        } else if (option.IDENTITY() != null) {
            Identity_valueContext identity = option.identity_value();
            if (identity == null) {
                col.setIdentity("1", "1");
            } else {
                col.setIdentity(identity.seed.getText(), identity.increment.getText());
            }
            if (option.not_for_rep != null) {
                col.setNotForRep(true);
            }
        } else if (option.MASKED() != null) {
            col.setMaskingFunction(option.STRING().getText());
        } else if (option.NULL() != null) {
            col.setNotNull(option.NOT() != null);
        } else if (option.column_constraint_body() != null) {
            fillColumnConstraint(option, col, stmt);
        } else if (option.INDEX() != null) {
            var index = new MsIndex(option.index.getText());
            var isTableIndex = stmt instanceof MsTable;
            index.setClustered(option.clustered() != null && option.clustered().CLUSTERED() != null);

            if (option.index_sort() == null) {
                index.addColumn(new SimpleColumn(col.getName()));
            } else {
                fillColumns(index, option.index_sort().column_name_list_with_order().column_with_order(), null, null);
            }

            if (option.index_options() != null) {
                fillOptions(index, option.index_options().index_option());
            }

            if (option.file_group_name != null) {
                index.setTablespace(MsDiffUtils.quoteName(option.file_group_name.getText()));
            } else if (isTableIndex && ((MsTable) stmt).getTablespace() != null) {
                index.setTablespace(((MsTable) stmt).getTablespace());
            }

            if (option.index_where() != null) {
                index.setWhere(getFullCtxText(option.index_where().where));
            }
            stmt.addChild(index);
        }
    }

    private MsGeneratedType getGenerated(Column_optionContext option) {
        boolean isStart = option.START() != null;
        if (option.ROW() != null) {
            return isStart ? MsGeneratedType.ROW_START : MsGeneratedType.ROW_END;
        }

        if (option.TRANSACTION_ID() != null) {
            return isStart ? MsGeneratedType.TRAN_START : MsGeneratedType.TRAN_END;
        }

        if (option.SEQUENCE_NUMBER() != null) {
            return isStart ? MsGeneratedType.SEQ_START : MsGeneratedType.SEQ_END;
        }

        throw new IllegalStateException("Unsupported GENERATED ALWAYS column type: " + getFullCtxText(option));
    }

    protected void fillColumns(ISimpleColumnContainer stmt, List<Column_with_orderContext> cols, String schema,
                               String table) {
        SimpleColumn simpCol;
        for (var col : cols) {
            var name = col.id().getText();
            simpCol = new SimpleColumn(name);
            var orderCtx = col.asc_desc();
            simpCol.setDesc(orderCtx != null && orderCtx.DESC() != null);
            stmt.addColumn(simpCol);
            if (schema != null && table != null) {
                stmt.addDependency(new ObjectReference(schema, table, name, DbObjType.COLUMN));
            }
        }
    }

    protected void fillOrderCols(MsIndex index, List<Column_with_orderContext> cols, String schema, String table) {
        for (var col : cols) {
            var colName = col.id().getText();
            index.addOrderCol(colName);
            index.addDependency(new ObjectReference(schema, table, colName, DbObjType.COLUMN));
        }
    }

    protected void addTypeDepcy(Data_typeContext ctx, AbstractStatement st) {
        Qualified_nameContext qname = ctx.qualified_name();
        if (qname != null && qname.schema != null) {
            addDepSafe(st, Arrays.asList(qname.schema, qname.name), DbObjType.TYPE);
        }
    }

    protected List<ParserRuleContext> getIdentifiers(Qualified_nameContext qNameCtx) {
        List<ParserRuleContext> ids = new ArrayList<>(2);
        ParserRuleContext schemaRule = qNameCtx.schema;
        if (schemaRule != null) {
            ids.add(schemaRule);
        }
        ids.add(qNameCtx.name);
        return ids;
    }

    protected String getType(Data_typeContext datatype) {
        String type = getFullCtxText(datatype);
        // backward compatibility, remove later
        if ("[nchar]".equalsIgnoreCase(type) || "[char]".equalsIgnoreCase(type)) {
            return type.concat(SIZE_ONE);
        }

        return type;
    }

    protected String buildDataSpace(DataspaceContext dataspaceCtx) {
        if (dataspaceCtx.filegroup != null) {
            return MsDiffUtils.quoteName(dataspaceCtx.filegroup.getText());
        }

        return MsDiffUtils.quoteName(dataspaceCtx.partition_table.getText())
                + "(" + MsDiffUtils.quoteName(dataspaceCtx.partition_col.getText()) + ")";
    }

    @Override
    protected MsSchema createSchema(String name) {
        return new MsSchema(name);
    }

    @Override
    protected boolean isSystemSchema(String schema) {
        return MsDiffUtils.isSystemSchema(schema);
    }

    @Override
    protected Path getRelativeFolderPath(IStatement st, Path baseDir) {
        return MsWorkDirs.getRelativeFolderPath(st, baseDir);
    }

    @Override
    protected List<String> getDirectoryNames() {
        return MsWorkDirs.getDirectoryNames();
    }

    @Override
    protected String getExportedFileName(IStatement st) {
        String fileName = super.getExportedFileName(st);
        if (st instanceof ISearchPath sp) {
            return FileUtils.getValidFilename(sp.getSchemaName()) + '.' + fileName;
        }
        return fileName;
    }

    @Override
    protected MsSchema getSchemaSafe(List<? extends ParserRuleContext> ids) {
        return (MsSchema) super.getSchemaSafe(ids);
    }
}
