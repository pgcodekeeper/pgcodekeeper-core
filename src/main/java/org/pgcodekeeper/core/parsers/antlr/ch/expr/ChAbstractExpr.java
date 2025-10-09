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
package org.pgcodekeeper.core.parsers.antlr.ch.expr;

import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.Utils;
import org.pgcodekeeper.core.database.base.parser.antlr.AbstractExpr;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.base.QNameParser;
import org.pgcodekeeper.core.parsers.antlr.ch.generated.CHParser.Alias_clauseContext;
import org.pgcodekeeper.core.parsers.antlr.ch.generated.CHParser.Qualified_nameContext;
import org.pgcodekeeper.core.schema.GenericColumn;
import org.pgcodekeeper.core.schema.meta.MetaContainer;

/**
 * Abstract base class for ClickHouse SQL expression parsing and dependency analysis.
 * Provides common functionality for tracking object references and dependencies.
 */
public abstract class ChAbstractExpr extends AbstractExpr {
    private final String schema;

    protected ChAbstractExpr(ChAbstractExpr parent) {
        super(parent);
        this.schema = parent.schema;
    }

    protected ChAbstractExpr(String schema, MetaContainer meta) {
        super(meta);
        this.schema = schema;
    }

    protected void addReferenceInRootParent(Qualified_nameContext name, Alias_clauseContext alias, boolean isFrom) {
        if (parent instanceof ChAbstractExpr chAbstractExpr) {
            chAbstractExpr.addReferenceInRootParent(name, alias, isFrom);
        }
    }

    @Override
    protected boolean isSystemSchema(String schema) {
        return Utils.isChSystemSchema(schema);
    }

    protected final GenericColumn addObjectDepcy(Qualified_nameContext qualifiedName) {
        var ids = qualifiedName.identifier();
        var schemaCtx = QNameParser.getSchemaNameCtx(ids);
        var relationName = QNameParser.getFirstName(ids);
        var relationCtx = QNameParser.getFirstNameCtx(ids);
        var schemaName = schemaCtx != null ? schemaCtx.getText() : Consts.CH_DEFAULT_DB;

        addDependency(new GenericColumn(schemaName, DbObjType.SCHEMA), schemaCtx);
        GenericColumn dependency = new GenericColumn(schemaName, relationName, DbObjType.TABLE);
        addDependency(dependency, relationCtx);
        return dependency;
    }
}
