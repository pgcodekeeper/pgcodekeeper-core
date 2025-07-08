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
package org.pgcodekeeper.core.parsers.antlr.statements.ch;

import java.util.Arrays;

import org.pgcodekeeper.core.DangerStatement;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.QNameParser;
import org.pgcodekeeper.core.parsers.antlr.generated.CHParser.Alter_table_actionContext;
import org.pgcodekeeper.core.parsers.antlr.generated.CHParser.Alter_table_stmtContext;
import org.pgcodekeeper.core.schema.AbstractSchema;
import org.pgcodekeeper.core.schema.AbstractTable;
import org.pgcodekeeper.core.schema.PgObjLocation;
import org.pgcodekeeper.core.schema.ch.ChDatabase;
import org.pgcodekeeper.core.settings.ISettings;

public final class AlterChTable extends ChParserAbstract {

    private final Alter_table_stmtContext ctx;

    public AlterChTable(Alter_table_stmtContext ctx, ChDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        var ids = getIdentifiers(ctx.qualified_name());
        var schemaCtx = QNameParser.getSchemaNameCtx(ids);
        var nameCtx = QNameParser.getFirstNameCtx(ids);
        AbstractTable table = getSafe(AbstractSchema::getTable, getSchemaSafe(ids), nameCtx);
        PgObjLocation loc = addObjReference(ids, DbObjType.TABLE, ACTION_ALTER);
        for (Alter_table_actionContext alterAction : ctx.alter_table_actions().alter_table_action()) {
            if (alterAction.UPDATE() != null) {
                loc.setWarning(DangerStatement.UPDATE);
            } else if (alterAction.DROP() != null && alterAction.alter_table_drop_action().COLUMN() != null) {
                loc.setWarning(DangerStatement.DROP_COLUMN);
            } else if (alterAction.MODIFY() != null && alterAction.alter_table_modify_action().COLUMN() != null) {
                loc.setWarning(DangerStatement.ALTER_COLUMN);
            }

            if (alterAction.ADD() == null) {
                continue;
            }

            var addAction = alterAction.alter_table_add_action();
            var constraintCtx = addAction.table_constraint_def();
            if (constraintCtx != null) {
                var constr = getConstraint(constraintCtx);
                addSafe(table, constr, Arrays.asList(schemaCtx, nameCtx, constraintCtx.identifier()));
            } else if (addAction.INDEX() != null) {
                var indexCtx = addAction.table_index_def();
                var index = getIndex(indexCtx);
                addSafe(table, index, Arrays.asList(schemaCtx, nameCtx, indexCtx.identifier()));
            }
        }
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_ALTER, DbObjType.TABLE, ctx.qualified_name());
    }
}
