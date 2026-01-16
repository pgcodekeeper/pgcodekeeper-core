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
package org.pgcodekeeper.core.database.pg.parser.statement;

import java.util.*;

import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.pg.parser.generated.SQLParser.*;
import org.pgcodekeeper.core.database.pg.schema.*;
import org.pgcodekeeper.core.settings.ISettings;

/**
 * Parser for PostgreSQL CREATE EVENT TRIGGER statements.
 * <p>
 * This class handles parsing of event trigger definitions including
 * trigger events (DDL_COMMAND_START, DDL_COMMAND_END, TABLE_REWRITE),
 * filter conditions using tag matching, and the executable function
 * that responds to the database events.
 */
public final class PgCreateEventTrigger extends PgParserAbstract {

    /**
     * Constant for the "tag" filter variable name.
     */
    public static final String TAG = "tag";

    private final Create_event_trigger_statementContext ctx;

    /**
     * Constructs a new CreateEventTrigger parser.
     *
     * @param ctx      the CREATE EVENT TRIGGER statement context
     * @param db       the PostgreSQL database object
     * @param settings the ISettings object
     */
    public PgCreateEventTrigger(Create_event_trigger_statementContext ctx, PgDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        PgEventTrigger eventTrigger = new PgEventTrigger(ctx.name.getText());
        eventTrigger.setEvent(ctx.event.getText());

        if (ctx.WHEN() != null) {
            for (Event_trigger_filter_variablesContext etFiltersCtx : ctx.event_trigger_filter_variables()) {
                if (TAG.equals(etFiltersCtx.identifier().getText().toLowerCase(Locale.ROOT))) {
                    for (var charCtx : etFiltersCtx.filter_values) {
                        eventTrigger.addTag(charCtx.getText());
                    }
                }
            }
        }

        eventTrigger.setExecutable(getFullCtxText(ctx.func_name));

        Schema_qualified_name_nontypeContext funcNameCtx = ctx.func_name.schema_qualified_name_nontype();
        if (funcNameCtx.schema != null) {
            addDepSafe(eventTrigger, getIdentifiers(funcNameCtx), DbObjType.FUNCTION, "()");
        }
        addSafe(db, eventTrigger, Collections.singletonList(ctx.name));
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_CREATE, DbObjType.EVENT_TRIGGER, Collections.singletonList(ctx.name));
    }
}
