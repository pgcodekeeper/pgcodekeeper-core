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
import org.pgcodekeeper.core.utils.Utils;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.base.QNameParser;
import org.pgcodekeeper.core.parsers.antlr.pg.launcher.RuleAnalysisLauncher;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.Create_rewrite_statementContext;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.Rewrite_commandContext;
import org.pgcodekeeper.core.database.base.schema.AbstractDatabase;
import org.pgcodekeeper.core.database.base.schema.AbstractSchema;
import org.pgcodekeeper.core.database.api.schema.EventType;
import org.pgcodekeeper.core.database.base.schema.AbstractStatementContainer;
import org.pgcodekeeper.core.database.pg.schema.PgDatabase;
import org.pgcodekeeper.core.database.pg.schema.PgRule;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Parser for PostgreSQL CREATE RULE statements.
 * <p>
 * This class handles parsing of rewrite rule definitions including rule events
 * (SELECT, INSERT, UPDATE, DELETE), conditions, and action commands.
 * Rules provide a mechanism for query rewriting and can implement views,
 * triggers, and other query transformations.
 */
public final class CreateRule extends PgParserAbstract {
    private final Create_rewrite_statementContext ctx;

    /**
     * Constructs a new CreateRule parser.
     *
     * @param ctx      the CREATE RULE statement context
     * @param db       the PostgreSQL database object
     * @param settings the ISettings object
     */
    public CreateRule(Create_rewrite_statementContext ctx, PgDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        List<ParserRuleContext> ids = getIdentifiers(ctx.table_name);
        addObjReference(ids, DbObjType.TABLE, null);

        PgRule rule = new PgRule(ctx.name.getText());
        rule.setEvent(EventType.valueOf(ctx.event.getText().toUpperCase(Locale.ROOT)));
        if (ctx.INSTEAD() != null) {
            rule.setInstead(true);
        }

        setConditionAndAddCommands(ctx, rule, db, fileName, settings);

        ParserRuleContext parent = QNameParser.getFirstNameCtx(ids);
        AbstractStatementContainer cont = getSafe(AbstractSchema::getStatementContainer,
                getSchemaSafe(ids), parent);
        addSafe(cont, rule, Arrays.asList(QNameParser.getSchemaNameCtx(ids), parent, ctx.name));
    }

    public static void setConditionAndAddCommands(Create_rewrite_statementContext ctx,
                                                  PgRule rule, AbstractDatabase db, String location, ISettings settings) {
        rule.setCondition((ctx.WHERE() != null) ? getFullCtxText(ctx.vex()) : null);

        // allows to write a common namespace-setup code with no copy-paste for each cmd type
        for (Rewrite_commandContext cmd : ctx.rewrite_command()) {
            rule.addCommand(Utils.checkNewLines(getFullCtxText(cmd), settings.isKeepNewlines()));
        }

        db.addAnalysisLauncher(new RuleAnalysisLauncher(rule, ctx, location));
    }

    @Override
    protected String getStmtAction() {
        List<ParserRuleContext> ids = new ArrayList<>(getIdentifiers(ctx.table_name));
        ids.add(ctx.name);
        return getStrForStmtAction(ACTION_CREATE, DbObjType.RULE, ids);
    }
}

