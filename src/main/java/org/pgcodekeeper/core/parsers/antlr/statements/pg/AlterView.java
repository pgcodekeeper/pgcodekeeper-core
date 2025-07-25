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
package org.pgcodekeeper.core.parsers.antlr.statements.pg;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.QNameParser;
import org.pgcodekeeper.core.parsers.antlr.expr.launcher.VexAnalysisLauncher;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.Alter_view_actionContext;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.Alter_view_statementContext;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.VexContext;
import org.pgcodekeeper.core.schema.AbstractSchema;
import org.pgcodekeeper.core.schema.pg.PgDatabase;
import org.pgcodekeeper.core.schema.pg.PgView;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.List;

/**
 * Parser for PostgreSQL ALTER VIEW statements.
 * <p>
 * This class handles parsing of view alterations including setting and dropping
 * default values for view columns. These operations affect how the view
 * behaves during INSERT operations.
 */
public final class AlterView extends PgParserAbstract {

    private final Alter_view_statementContext ctx;
    private final CommonTokenStream stream;

    /**
     * Constructs a new AlterView parser.
     *
     * @param ctx      the ALTER VIEW statement context
     * @param db       the PostgreSQL database object
     * @param stream   the token stream for parsing
     * @param settings the ISettings object
     */
    public AlterView(Alter_view_statementContext ctx, PgDatabase db, CommonTokenStream stream, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
        this.stream = stream;
    }

    @Override
    public void parseObject() {
        List<ParserRuleContext> ids = getIdentifiers(ctx.name);
        var st = getSafe(AbstractSchema::getView, getSchemaSafe(ids), QNameParser.getFirstNameCtx(ids));
        if (st instanceof PgView dbView) {
            Alter_view_actionContext action = ctx.alter_view_action();
            if (action.set_def_column() != null) {
                VexContext exp = action.set_def_column().vex();
                doSafe((s, o) -> {
                    s.addColumnDefaultValue(getFullCtxText(action.column_name), getExpressionText(exp, stream));
                    db.addAnalysisLauncher(new VexAnalysisLauncher(s, exp, fileName));
                }, dbView, null);
            }
            if (action.drop_def() != null) {
                doSafe(PgView::removeColumnDefaultValue, dbView, getFullCtxText(action.column_name));
            }
        }

        addObjReference(ids, DbObjType.VIEW, ACTION_ALTER);
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_ALTER, DbObjType.VIEW, getIdentifiers(ctx.name));
    }
}
