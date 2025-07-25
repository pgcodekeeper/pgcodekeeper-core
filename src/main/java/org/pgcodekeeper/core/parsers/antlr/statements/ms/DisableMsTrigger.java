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

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.generated.TSQLParser.Enable_disable_triggerContext;
import org.pgcodekeeper.core.parsers.antlr.generated.TSQLParser.IdContext;
import org.pgcodekeeper.core.parsers.antlr.generated.TSQLParser.Names_referencesContext;
import org.pgcodekeeper.core.parsers.antlr.generated.TSQLParser.Qualified_nameContext;
import org.pgcodekeeper.core.schema.AbstractSchema;
import org.pgcodekeeper.core.schema.PgObjLocation;
import org.pgcodekeeper.core.schema.PgStatementContainer;
import org.pgcodekeeper.core.schema.ms.MsDatabase;
import org.pgcodekeeper.core.schema.ms.MsTrigger;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.Arrays;

/**
 * Parser for Microsoft SQL ENABLE/DISABLE TRIGGER statements.
 * Handles trigger activation and deactivation on tables including
 * table and trigger reference tracking.
 */
public final class DisableMsTrigger extends MsParserAbstract {

    private final Enable_disable_triggerContext ctx;

    /**
     * Creates a parser for Microsoft SQL ENABLE/DISABLE TRIGGER statements.
     *
     * @param ctx      the ANTLR parse tree context for the ENABLE/DISABLE TRIGGER statement
     * @param db       the Microsoft SQL database schema being processed
     * @param settings parsing configuration settings
     */
    public DisableMsTrigger(Enable_disable_triggerContext ctx, MsDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        Names_referencesContext triggers = ctx.names_references();
        Qualified_nameContext parent = ctx.qualified_name();
        if (triggers == null || parent == null) {
            return;
        }

        IdContext schemaCtx = parent.schema;
        PgStatementContainer cont = getSafe(AbstractSchema::getStatementContainer,
                getSchemaSafe(Arrays.asList(schemaCtx, parent.name)), parent.name);
        addObjReference(Arrays.asList(parent.schema, parent.name),
                DbObjType.TABLE, null);

        for (Qualified_nameContext qname : triggers.qualified_name()) {
            MsTrigger trig = (MsTrigger) getSafe(PgStatementContainer::getTrigger,
                    cont, qname.name);
            addObjReference(Arrays.asList(schemaCtx, parent.name, qname.name),
                    DbObjType.TRIGGER, ACTION_ALTER);
            if (ctx.DISABLE() != null) {
                doSafe(MsTrigger::setDisable, trig, true);
            }
        }
    }

    @Override
    protected PgObjLocation fillQueryLocation(ParserRuleContext ctx) {
        StringBuilder sb = new StringBuilder();
        Enable_disable_triggerContext ctxEnableDisableTr = (Enable_disable_triggerContext) ctx;
        sb.append(ctxEnableDisableTr.DISABLE() != null ? "DISABLE " : "ENABLE ")
                .append("TRIGGER");

        Names_referencesContext triggers = ctxEnableDisableTr.names_references();
        Qualified_nameContext parent = ctxEnableDisableTr.qualified_name();

        if (triggers != null && parent != null) {
            sb.append(' ');

            String schemaName = parent.schema.getText();
            String parentName = parent.name.getText();

            for (Qualified_nameContext qname : triggers.qualified_name()) {
                sb.append(schemaName)
                        .append('.').append(parentName)
                        .append('.').append(qname.name.getText())
                        .append(", ");
            }

            sb.setLength(sb.length() - 2);
        }

        PgObjLocation loc = new PgObjLocation.Builder()
                .setAction(sb.toString())
                .setCtx(ctx)
                .setSql(getFullCtxText(ctx))
                .build();

        db.addReference(fileName, loc);
        return loc;
    }

    @Override
    protected String getStmtAction() {
        return null;
    }
}
