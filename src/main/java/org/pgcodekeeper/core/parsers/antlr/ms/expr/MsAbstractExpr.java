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

import org.pgcodekeeper.core.Utils;
import org.pgcodekeeper.core.database.base.parser.antlr.AbstractExpr;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.Data_typeContext;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.Full_column_nameContext;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.IdContext;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.Qualified_nameContext;
import org.pgcodekeeper.core.database.api.schema.GenericColumn;
import org.pgcodekeeper.core.database.api.schema.IRelation;
import org.pgcodekeeper.core.database.base.schema.meta.MetaContainer;
import org.pgcodekeeper.core.utils.Pair;

/**
 * Abstract base class for Microsoft SQL expression analysis.
 * Provides common functionality for parsing and analyzing SQL expressions,
 * managing dependencies, and handling database object references.
 */
public abstract class MsAbstractExpr extends AbstractExpr {

    private final String schema;

    protected MsAbstractExpr(String schema, MetaContainer meta) {
        super(meta);
        this.schema = schema;
    }

    protected MsAbstractExpr(MsAbstractExpr parent) {
        super(parent);
        this.schema = parent.schema;
    }

    protected GenericColumn addObjectDepcy(Qualified_nameContext qualifiedName, DbObjType type) {
        IdContext nameCtx = qualifiedName.name;
        String relationName = nameCtx.getText();
        IdContext schemaCtx = qualifiedName.schema;
        String schemaName;
        if (schemaCtx == null) {
            schemaName = schema;
        } else {
            schemaName = schemaCtx.getText();
            addDependency(new GenericColumn(schemaName, DbObjType.SCHEMA), schemaCtx);
        }

        GenericColumn depcy = new GenericColumn(schemaName, relationName, type);
        addDependency(depcy, nameCtx);
        return depcy;
    }

    protected void addTypeDepcy(Data_typeContext dt) {
        Qualified_nameContext name = dt.qualified_name();
        if (name != null && name.schema != null
                && !isSystemSchema(name.schema.getText())) {
            addObjectDepcy(name, DbObjType.TYPE);
        }
    }

    protected boolean isSystemSchema(String schema) {
        return Utils.isMsSystemSchema(schema);
    }

    protected void addColumnDepcy(Full_column_nameContext fcn) {
        Qualified_nameContext tableName = fcn.qualified_name();
        IdContext columnCtx = fcn.id();
        String columnName = columnCtx.getText();

        if (tableName == null) {
            Pair<IRelation, Pair<String, String>> relCol = findColumn(columnName);
            if (relCol == null) {
                return;
            }

            IRelation rel = relCol.getFirst();
            Pair<String, String> col = relCol.getSecond();
            addDependency(new GenericColumn(rel.getSchemaName(), rel.getName(), col.getFirst(), DbObjType.COLUMN),
                    columnCtx);
            return;
        }

        IdContext schemaCtx = tableName.schema;
        String schemaName = null;
        if (schemaCtx != null) {
            schemaName = schemaCtx.getText();
            addDependency(new GenericColumn(schemaName, DbObjType.SCHEMA), schemaCtx);
        }

        IdContext relationCtx = tableName.name;
        String relationName = relationCtx.getText();

        var ref = findReference(schemaName, relationName, null);
        if (ref != null) {
            GenericColumn table = ref.getValue();
            if (table != null) {
                if (relationName.equals(table.table())) {
                    addDependency(table, relationCtx);
                } else {
                    addReference(table, relationCtx);
                }

                addDependency(new GenericColumn(table.schema(), table.table(),
                        columnName, DbObjType.COLUMN), columnCtx);
            }
        } else {
            log(relationCtx, Messages.AbstractExpr_log_unknown_column_ref, schemaName, relationName, columnName);
        }
    }
}