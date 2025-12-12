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
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.base.QNameParser;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.Alter_materialized_view_statementContext;
import org.pgcodekeeper.core.database.base.schema.AbstractIndex;
import org.pgcodekeeper.core.database.base.schema.AbstractSchema;
import org.pgcodekeeper.core.database.api.schema.ObjectLocation;
import org.pgcodekeeper.core.database.pg.schema.PgAbstractView;
import org.pgcodekeeper.core.database.pg.schema.PgDatabase;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.List;

/**
 * Parser for PostgreSQL ALTER MATERIALIZED VIEW statements.
 * <p>
 * This class handles parsing of materialized view alterations including
 * setting clustered indexes and handling ALTER MATERIALIZED VIEW ALL
 * operations that affect all materialized views in a tablespace.
 */
public final class AlterMatView extends PgParserAbstract {

    private final Alter_materialized_view_statementContext ctx;
    private final String action;

    /**
     * Constructs a new AlterMatView parser.
     *
     * @param ctx      the ALTER MATERIALIZED VIEW statement context
     * @param db       the PostgreSQL database object
     * @param settings the ISettings object
     */
    public AlterMatView(Alter_materialized_view_statementContext ctx, PgDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
        this.action = ctx.ALL() != null ? "ALTER MATERIALIZED VIEW ALL" : "ALTER MATERIALIZED";
    }

    @Override
    public void parseObject() {
        if (ctx.ALL() == null) {
            List<ParserRuleContext> ids = getIdentifiers(ctx.schema_qualified_name());
            addObjReference(ids, DbObjType.VIEW, action);

            PgAbstractView view = (PgAbstractView) getSafe(AbstractSchema::getView,
                    getSchemaSafe(ids), QNameParser.getFirstNameCtx(ids));

            var alterAction = ctx.alter_materialized_view_action();
            if (alterAction != null) {
                for (var act : alterAction.materialized_view_action()) {
                    var indexNameCtx = act.index_name;
                    if (indexNameCtx != null) {
                        ParserRuleContext indexName = QNameParser.getFirstNameCtx(getIdentifiers(indexNameCtx));
                        AbstractIndex index = getSafe(PgAbstractView::getIndex, view, indexName);
                        doSafe(AbstractIndex::setClustered, index, true);
                    }
                }
            }
        } else {
            db.addReference(fileName, new ObjectLocation.Builder()
                    .setAction(action).setCtx(ctx.getParent()).build());
        }
    }

    @Override
    protected String getStmtAction() {
        if (ctx.ALL() != null) {
            return action;
        }
        return getStrForStmtAction(action, DbObjType.VIEW,
                ctx.schema_qualified_name().identifier());
    }
}