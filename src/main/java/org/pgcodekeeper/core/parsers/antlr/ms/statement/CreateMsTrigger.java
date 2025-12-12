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

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.ms.launcher.MsFuncProcTrigAnalysisLauncher;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.Batch_statementContext;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.Create_or_alter_triggerContext;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.IdContext;
import org.pgcodekeeper.core.database.base.schema.AbstractSchema;
import org.pgcodekeeper.core.database.base.schema.AbstractStatementContainer;
import org.pgcodekeeper.core.database.ms.schema.MsDatabase;
import org.pgcodekeeper.core.database.ms.schema.MsTrigger;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.Arrays;
import java.util.List;

/**
 * Parser for Microsoft SQL CREATE TRIGGER statements.
 * Handles trigger creation including ANSI_NULLS and QUOTED_IDENTIFIER settings,
 * table references, and trigger body analysis.
 */
public final class CreateMsTrigger extends BatchContextProcessor {

    private final Create_or_alter_triggerContext ctx;

    private final boolean ansiNulls;
    private final boolean quotedIdentifier;

    /**
     * Creates a parser for Microsoft SQL CREATE TRIGGER statements.
     *
     * @param ctx              the batch statement context containing the trigger definition
     * @param db               the Microsoft SQL database schema being processed
     * @param ansiNulls        the ANSI_NULLS setting for the trigger
     * @param quotedIdentifier the QUOTED_IDENTIFIER setting for the trigger
     * @param stream           the token stream for source code processing
     * @param settings         parsing configuration settings
     */
    public CreateMsTrigger(Batch_statementContext ctx, MsDatabase db,
                           boolean ansiNulls, boolean quotedIdentifier, CommonTokenStream stream, ISettings settings) {
        super(db, ctx, stream, settings);
        this.ctx = ctx.batch_statement_body().create_or_alter_trigger();
        this.ansiNulls = ansiNulls;
        this.quotedIdentifier = quotedIdentifier;
    }

    @Override
    protected ParserRuleContext getDelimiterCtx() {
        return ctx.table_name;
    }

    @Override
    public void parseObject() {
        IdContext schemaCtx = ctx.trigger_name.schema;
        if (schemaCtx == null) {
            schemaCtx = ctx.table_name.schema;
        }
        List<ParserRuleContext> ids = Arrays.asList(schemaCtx, ctx.table_name.name);
        addObjReference(ids, DbObjType.TABLE, null);
        getObject(getSchemaSafe(ids), false);
    }

    /**
     * Creates and configures the trigger object from the parse context.
     * Handles schema resolution, source parts, and trigger analysis setup.
     *
     * @param schema the schema containing the table
     * @param isJdbc whether this is being parsed in JDBC mode
     * @return the created trigger object
     */
    public MsTrigger getObject(AbstractSchema schema, boolean isJdbc) {
        IdContext schemaCtx = ctx.trigger_name.schema;
        if (schemaCtx == null) {
            schemaCtx = ctx.table_name.schema;
        }
        IdContext tableNameCtx = ctx.table_name.name;
        IdContext nameCtx = ctx.trigger_name.name;

        MsTrigger trigger = new MsTrigger(nameCtx.getText());
        trigger.setAnsiNulls(ansiNulls);
        trigger.setQuotedIdentified(quotedIdentifier);
        setSourceParts(trigger);

        if (schema == null) {
            addObjReference(Arrays.asList(schemaCtx, tableNameCtx), DbObjType.TABLE, null);
        }

        db.addAnalysisLauncher(new MsFuncProcTrigAnalysisLauncher(trigger,
                ctx.sql_clauses(), fileName, settings.isEnableFunctionBodiesDependencies()));

        AbstractStatementContainer cont = getSafe(AbstractSchema::getStatementContainer,
                schema, tableNameCtx);

        if (isJdbc && schema != null) {
            cont.addTrigger(trigger);
        } else {
            addSafe(cont, trigger,
                    Arrays.asList(schemaCtx, tableNameCtx, nameCtx));
        }
        return trigger;
    }

    @Override
    protected String getStmtAction() {
        IdContext schemaCtx = ctx.trigger_name.schema;
        if (schemaCtx == null) {
            schemaCtx = ctx.table_name.schema;
        }
        return getStrForStmtAction(ACTION_CREATE, DbObjType.TRIGGER,
                Arrays.asList(schemaCtx, ctx.table_name.name, ctx.trigger_name.name));
    }
}
