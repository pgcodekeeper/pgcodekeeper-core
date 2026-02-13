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
package org.pgcodekeeper.core.database.ms.parser.expr;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.api.schema.meta.IMetaContainer;
import org.pgcodekeeper.core.database.base.parser.antlr.AbstractExpr;
import org.pgcodekeeper.core.database.ms.MsDiffUtils;
import org.pgcodekeeper.core.database.ms.parser.generated.TSQLParser.*;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.utils.Pair;

/**
 * Abstract base class for Microsoft SQL expression analysis.
 * Provides common functionality for parsing and analyzing SQL expressions,
 * managing dependencies, and handling database object references.
 */
public abstract class MsAbstractExpr extends AbstractExpr {

    private final String schema;

    protected MsAbstractExpr(String schema, IMetaContainer meta) {
        super(meta);
        this.schema = schema;
    }

    protected MsAbstractExpr(MsAbstractExpr parent) {
        super(parent);
        this.schema = parent.schema;
    }

    protected ObjectReference addObjectDepcy(Qualified_nameContext qualifiedName, DbObjType type) {
        IdContext nameCtx = qualifiedName.name;
        String relationName = nameCtx.getText();
        IdContext schemaCtx = qualifiedName.schema;
        String schemaName;
        if (schemaCtx == null) {
            schemaName = schema;
        } else {
            schemaName = schemaCtx.getText();
            addDependency(new ObjectReference(schemaName, DbObjType.SCHEMA), schemaCtx);
        }

        ObjectReference depcy = new ObjectReference(schemaName, relationName, type);
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

    @Override
    protected boolean isSystemSchema(String schema) {
        return MsDiffUtils.isSystemSchema(schema);
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
            addDependency(new ObjectReference(rel.getSchemaName(), rel.getName(), col.getFirst(), DbObjType.COLUMN),
                    columnCtx);
            return;
        }

        IdContext schemaCtx = tableName.schema;
        String schemaName = null;
        if (schemaCtx != null) {
            schemaName = schemaCtx.getText();
            addDependency(new ObjectReference(schemaName, DbObjType.SCHEMA), schemaCtx);
        }

        IdContext relationCtx = tableName.name;
        String relationName = relationCtx.getText();

        var ref = findReference(schemaName, relationName, null);
        if (ref != null) {
            ObjectReference table = ref.getValue();
            if (table != null) {
                if (relationName.equals(table.table())) {
                    addDependency(table, relationCtx);
                } else {
                    addReference(table, relationCtx);
                }

                addDependency(new ObjectReference(table.schema(), table.table(),
                        columnName, DbObjType.COLUMN), columnCtx);
            }
        } else {
            log(relationCtx, Messages.AbstractExpr_log_unknown_column_ref, schemaName, relationName, columnName);
        }
    }
}