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
package org.pgcodekeeper.core.parsers.antlr.pg.statement;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.database.pg.PgDiffUtils;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.base.QNameParser;
import org.pgcodekeeper.core.parsers.antlr.pg.launcher.TriggerAnalysisLauncher;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.*;
import org.pgcodekeeper.core.database.base.schema.AbstractDatabase;
import org.pgcodekeeper.core.database.base.schema.AbstractSchema;
import org.pgcodekeeper.core.database.base.schema.AbstractStatementContainer;
import org.pgcodekeeper.core.database.pg.schema.PgDatabase;
import org.pgcodekeeper.core.database.pg.schema.PgTrigger;
import org.pgcodekeeper.core.database.pg.schema.PgTrigger.TgTypes;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Parser for PostgreSQL CREATE TRIGGER statements.
 * <p>
 * This class handles parsing of trigger definitions including trigger timing
 * (BEFORE, AFTER, INSTEAD OF), events (INSERT, UPDATE, DELETE, TRUNCATE),
 * trigger functions, referencing clauses, and constraint triggers.
 */
public final class CreateTrigger extends PgParserAbstract {

    private final Create_trigger_statementContext ctx;

    /**
     * Constructs a new CreateTrigger parser.
     *
     * @param ctx      the CREATE TRIGGER statement context
     * @param db       the PostgreSQL database object
     * @param settings the ISettings object
     */
    public CreateTrigger(Create_trigger_statementContext ctx, PgDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        List<ParserRuleContext> ids = getIdentifiers(ctx.table_name);
        addObjReference(ids, DbObjType.TABLE, null);

        PgTrigger trigger = new PgTrigger(ctx.name.getText());
        if (ctx.AFTER() != null) {
            trigger.setType(TgTypes.AFTER);
        } else if (ctx.BEFORE() != null) {
            trigger.setType(TgTypes.BEFORE);
        } else if (ctx.INSTEAD() != null) {
            trigger.setType(TgTypes.INSTEAD_OF);
        }
        if (ctx.ROW() != null) {
            trigger.setForEachRow(true);
        }
        if (ctx.STATEMENT() != null) {
            trigger.setForEachRow(false);
        }
        trigger.setOnDelete(ctx.delete_true != null);
        trigger.setOnInsert(ctx.insert_true != null);
        trigger.setOnUpdate(ctx.update_true != null);
        trigger.setOnTruncate(ctx.truncate_true != null);
        trigger.setFunction(getFullCtxText(ctx.func_name));

        if (ctx.CONSTRAINT() != null) {
            trigger.setConstraint(true);
            Table_deferrableContext def = ctx.table_deferrable();
            if (def != null && def.NOT() == null) {
                Table_initialy_immedContext initImmed = ctx.table_initialy_immed();
                if (initImmed != null) {
                    trigger.setImmediate(initImmed.DEFERRED() == null);
                }
            }

            if (ctx.referenced_table_name != null) {
                List<ParserRuleContext> refName = getIdentifiers(ctx.referenced_table_name);
                String refSchemaName = QNameParser.getSecondName(refName);
                String refRelName = QNameParser.getFirstName(refName);

                StringBuilder sb = new StringBuilder();
                if (refSchemaName == null) {
                    refSchemaName = getSchemaNameSafe(ids);
                }

                if (refSchemaName != null) {
                    sb.append(PgDiffUtils.getQuotedName(refSchemaName)).append('.');
                }
                sb.append(PgDiffUtils.getQuotedName(refRelName));

                addDepSafe(trigger, refName, DbObjType.TABLE);
                trigger.setRefTableName(sb.toString());
            }
        }

        for (Trigger_referencingContext ref : ctx.trigger_referencing()) {
            String name = ref.identifier().getText();
            if (ref.NEW() != null) {
                trigger.setNewTable(name);
            } else {
                trigger.setOldTable(name);
            }
        }

        Schema_qualified_name_nontypeContext funcNameCtx = ctx.func_name
                .schema_qualified_name_nontype();
        if (funcNameCtx.schema != null) {
            addDepSafe(trigger, getIdentifiers(funcNameCtx), DbObjType.FUNCTION, "()");
        }

        ParserRuleContext schemaCtx = QNameParser.getSchemaNameCtx(ids);
        ParserRuleContext parentCtx = QNameParser.getFirstNameCtx(ids);

        for (Identifier_listContext column : ctx.identifier_list()) {
            for (IdentifierContext nameCol : column.identifier()) {
                trigger.addUpdateColumn(nameCol.getText());
                addDepSafe(trigger, Arrays.asList(schemaCtx, parentCtx, nameCol), DbObjType.COLUMN);
            }
        }
        parseWhen(ctx.when_trigger(), trigger, db, fileName);

        AbstractStatementContainer cont = getSafe(AbstractSchema::getStatementContainer,
                getSchemaSafe(ids), parentCtx);
        addSafe(cont, trigger, Arrays.asList(schemaCtx, parentCtx, ctx.name));
    }

    /**
     * Parses the WHEN clause of a trigger definition.
     * <p>
     * This method processes trigger conditions that determine when the trigger
     * should fire based on the values in the affected row.
     *
     * @param whenCtx  the WHEN trigger context, may be null
     * @param trigger  the trigger object to configure
     * @param db       the database for analysis launchers
     * @param location the source location for error reporting
     */
    public static void parseWhen(When_triggerContext whenCtx, PgTrigger trigger,
                                 AbstractDatabase db, String location) {
        if (whenCtx != null) {
            VexContext vex = whenCtx.vex();
            trigger.setWhen(getFullCtxText(vex));
            db.addAnalysisLauncher(new TriggerAnalysisLauncher(trigger, vex, location));
        }
    }

    @Override
    protected String getStmtAction() {
        List<ParserRuleContext> ids = new ArrayList<>(getIdentifiers(ctx.table_name));
        ids.add(ctx.name);
        return getStrForStmtAction(ACTION_CREATE, DbObjType.TRIGGER, ids);
    }
}
