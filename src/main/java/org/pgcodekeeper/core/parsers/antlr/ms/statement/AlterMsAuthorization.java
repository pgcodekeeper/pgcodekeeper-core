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
package org.pgcodekeeper.core.parsers.antlr.ms.statement;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.Alter_authorizationContext;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.Class_typeContext;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.IdContext;
import org.pgcodekeeper.core.database.base.schema.AbstractSchema;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.database.base.schema.StatementOverride;
import org.pgcodekeeper.core.database.ms.schema.MsDatabase;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Parser for Microsoft SQL ALTER AUTHORIZATION statements.
 * Handles ownership changes for database objects including tables, assemblies, roles, and schemas
 * with support for statement overrides.
 */
public final class AlterMsAuthorization extends MsParserAbstract {

    private final Alter_authorizationContext ctx;
    private final Map<AbstractStatement, StatementOverride> overrides;

    /**
     * Creates a parser for Microsoft SQL ALTER AUTHORIZATION statements without overrides.
     *
     * @param ctx      the ANTLR parse tree context for the ALTER AUTHORIZATION statement
     * @param db       the Microsoft SQL database schema being processed
     * @param settings parsing configuration settings
     */
    public AlterMsAuthorization(Alter_authorizationContext ctx, MsDatabase db, ISettings settings) {
        this(ctx, db, null, settings);
    }

    /**
     * Creates a parser for Microsoft SQL ALTER AUTHORIZATION statements with statement overrides.
     *
     * @param ctx       the ANTLR parse tree context for the ALTER AUTHORIZATION statement
     * @param db        the Microsoft SQL database schema being processed
     * @param overrides map of statement overrides for ownership modifications
     * @param settings  parsing configuration settings
     */
    public AlterMsAuthorization(Alter_authorizationContext ctx, MsDatabase db,
                                Map<AbstractStatement, StatementOverride> overrides, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
        this.overrides = overrides;
    }

    @Override
    public void parseObject() {
        IdContext ownerId = ctx.id();
        if (settings.isIgnorePrivileges() || ownerId == null) {
            return;
        }
        String owner = ownerId.getText();

        Class_typeContext type = ctx.class_type();
        IdContext nameCtx = ctx.qualified_name().name;
        IdContext schemaCtx = ctx.qualified_name().schema;
        List<ParserRuleContext> ids = Arrays.asList(schemaCtx, nameCtx);

        AbstractStatement st = null;
        if (type == null || type.OBJECT() != null || type.TYPE() != null) {
            AbstractSchema schema = getSchemaSafe(ids);
            st = getSafe((k, v) -> k.getChildren().filter(
                            e -> e.getBareName().equals(v))
                    .findAny().orElse(null), schema, nameCtx);

            // when type is not defined (sometimes in ref mode), suppose it is a table
            addObjReference(Arrays.asList(schemaCtx, nameCtx),
                    st != null ? st.getStatementType() : DbObjType.TABLE, ACTION_ALTER);
        } else if (type.ASSEMBLY() != null) {
            st = getSafe(MsDatabase::getAssembly, db, nameCtx);
            addObjReference(List.of(nameCtx), DbObjType.ASSEMBLY, ACTION_ALTER);
        } else if (type.ROLE() != null) {
            st = getSafe(MsDatabase::getRole, db, nameCtx);
            addObjReference(List.of(nameCtx), DbObjType.ROLE, ACTION_ALTER);
        } else if (type.SCHEMA() != null) {
            st = getSafe(MsDatabase::getSchema, db, nameCtx);
            addObjReference(List.of(nameCtx), DbObjType.SCHEMA, ACTION_ALTER);
        }

        if (st != null) {
            setOwner(st, owner);
        }
    }

    private void setOwner(AbstractStatement st, String owner) {
        if (overrides == null) {
            st.setOwner(owner);
        } else {
            overrides.computeIfAbsent(st, k -> new StatementOverride()).setOwner(owner);
        }
    }

    @Override
    protected String getStmtAction() {
        return ACTION_ALTER + " AUTHORIZATION";
    }
}
