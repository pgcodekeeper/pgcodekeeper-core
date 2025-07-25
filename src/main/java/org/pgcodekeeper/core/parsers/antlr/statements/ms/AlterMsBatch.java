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
package org.pgcodekeeper.core.parsers.antlr.statements.ms;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.generated.TSQLParser.Batch_statement_bodyContext;
import org.pgcodekeeper.core.parsers.antlr.generated.TSQLParser.Create_or_alter_triggerContext;
import org.pgcodekeeper.core.parsers.antlr.generated.TSQLParser.IdContext;
import org.pgcodekeeper.core.parsers.antlr.generated.TSQLParser.Qualified_nameContext;
import org.pgcodekeeper.core.schema.PgObjLocation;
import org.pgcodekeeper.core.schema.ms.MsDatabase;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.Arrays;
import java.util.List;

/**
 * Parser for Microsoft SQL ALTER statements in batch context.
 * Handles ALTER PROCEDURE, ALTER FUNCTION, ALTER VIEW, and ALTER TRIGGER statements
 * within batch processing context with proper location tracking.
 */
public final class AlterMsBatch extends BatchContextProcessor {

    private final Batch_statement_bodyContext ctx;

    /**
     * Creates a parser for Microsoft SQL ALTER statements in batch context.
     *
     * @param ctx      the batch statement body context containing the ALTER statement
     * @param db       the Microsoft SQL database schema being processed
     * @param stream   the token stream for source code processing
     * @param settings parsing configuration settings
     */
    public AlterMsBatch(Batch_statement_bodyContext ctx, MsDatabase db, CommonTokenStream stream, ISettings settings) {
        super(db, ctx, stream, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        if (ctx.create_or_alter_procedure() != null) {
            alter(ctx.create_or_alter_procedure().qualified_name(), DbObjType.PROCEDURE);
        } else if (ctx.create_or_alter_function() != null) {
            alter(ctx.create_or_alter_function().qualified_name(), DbObjType.FUNCTION);
        } else if (ctx.create_or_alter_view() != null) {
            alter(ctx.create_or_alter_view().qualified_name(), DbObjType.VIEW);
        } else if (ctx.create_or_alter_trigger() != null) {
            alterTrigger(ctx.create_or_alter_trigger());
        }
    }

    private void alter(Qualified_nameContext qname, DbObjType type) {
        addObjReference(Arrays.asList(qname.schema, qname.name), type, ACTION_ALTER);
    }

    private void alterTrigger(Create_or_alter_triggerContext ctx) {
        Qualified_nameContext qname = ctx.trigger_name;
        IdContext schemaCtx = qname.schema;
        IdContext secondCtx = ctx.table_name.schema;
        if (schemaCtx == null) {
            schemaCtx = secondCtx;
        }

        // second schema ref
        // CREATE TRIGGER schema.trigger ON schema.table ...
        if (secondCtx != null) {
            addObjReference(List.of(secondCtx), DbObjType.SCHEMA, null);
        }
        addObjReference(Arrays.asList(schemaCtx, ctx.table_name.name),
                DbObjType.TABLE, null);
        addObjReference(Arrays.asList(schemaCtx, ctx.table_name.name, qname.name),
                DbObjType.TRIGGER, ACTION_ALTER);
    }

    @Override
    protected PgObjLocation fillQueryLocation(ParserRuleContext ctx) {
        return super.fillQueryLocation(ctx.getParent());
    }

    @Override
    protected String getStmtAction() {
        List<? extends ParserRuleContext> ids;
        if (ctx.create_or_alter_procedure() != null) {
            Qualified_nameContext qname = ctx.create_or_alter_procedure().qualified_name();
            return getStrForStmtAction(ACTION_ALTER, DbObjType.PROCEDURE, qname);
        }
        if (ctx.create_or_alter_function() != null) {
            Qualified_nameContext qname = ctx.create_or_alter_function().qualified_name();
            return getStrForStmtAction(ACTION_ALTER, DbObjType.FUNCTION, qname);
        }
        if (ctx.create_or_alter_view() != null) {
            Qualified_nameContext qname = ctx.create_or_alter_view().qualified_name();
            return getStrForStmtAction(ACTION_ALTER, DbObjType.VIEW, qname);
        }
        if (ctx.create_or_alter_trigger() != null) {
            Create_or_alter_triggerContext trigCtx = ctx.create_or_alter_trigger();
            Qualified_nameContext qname = trigCtx.trigger_name;
            IdContext schemaCtx = qname.schema;
            IdContext secondCtx = trigCtx.table_name.schema;
            if (schemaCtx == null) {
                schemaCtx = secondCtx;
            }
            ids = Arrays.asList(schemaCtx, trigCtx.table_name.name, qname.name);
            return getStrForStmtAction(ACTION_ALTER, DbObjType.TRIGGER, ids);
        }
        return null;
    }

    @Override
    protected ParserRuleContext getDelimiterCtx() {
        throw new IllegalStateException("Unsupported operation for AlterMsBatch");
    }
}