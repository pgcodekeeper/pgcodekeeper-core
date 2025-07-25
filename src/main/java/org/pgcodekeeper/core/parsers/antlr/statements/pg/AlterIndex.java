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

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.QNameParser;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.Alter_index_statementContext;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.Schema_qualified_nameContext;
import org.pgcodekeeper.core.schema.AbstractSchema;
import org.pgcodekeeper.core.schema.PgObjLocation;
import org.pgcodekeeper.core.schema.pg.PgDatabase;
import org.pgcodekeeper.core.schema.pg.PgIndex;
import org.pgcodekeeper.core.schema.pg.PgSchema;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.List;

/**
 * Parser for PostgreSQL ALTER INDEX statements.
 * <p>
 * This class handles parsing of index alterations including index inheritance
 * operations and ALTER INDEX ALL statements that affect all indexes in a tablespace.
 */
public final class AlterIndex extends PgParserAbstract {

    private final Alter_index_statementContext ctx;
    private final String alterIdxAllAction;

    /**
     * Constructs a new AlterIndex parser.
     *
     * @param ctx      the ALTER INDEX statement context
     * @param db       the PostgreSQL database object
     * @param settings the ISettings object
     */
    public AlterIndex(Alter_index_statementContext ctx, PgDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
        alterIdxAllAction = ctx.ALL() == null ? null : "ALTER INDEX ALL";
    }

    @Override
    public void parseObject() {
        if (alterIdxAllAction != null) {
            PgObjLocation loc = new PgObjLocation.Builder()
                    .setAction(alterIdxAllAction)
                    .setCtx(ctx.getParent())
                    .build();

            db.addReference(fileName, loc);
            return;
        }

        List<ParserRuleContext> ids = getIdentifiers(ctx.schema_qualified_name());

        Schema_qualified_nameContext inherit = ctx.index_def_action().index;

        if (inherit != null) {
            // in this case inherit is real index name
            List<ParserRuleContext> idsInh = getIdentifiers(inherit);
            PgSchema schema = getSchemaSafe(idsInh);
            ParserRuleContext inhName = QNameParser.getFirstNameCtx(idsInh);

            String inhSchemaName = getSchemaNameSafe(ids);
            String inhTableName = QNameParser.getFirstName(ids);

            addObjReference(idsInh, DbObjType.INDEX, ACTION_ALTER);
            if (schema == null) {
                return;
            }

            PgIndex index = (PgIndex) schema.getIndexByName(inhName.getText());
            if (index == null) {
                getSafe(AbstractSchema::getConstraintByName, schema, inhName);
            } else {
                doSafe((i, o) -> i.addInherit(inhSchemaName, inhTableName), index, null);
                addDepSafe(index, ids, DbObjType.INDEX);
            }

        } else {
            addObjReference(ids, DbObjType.INDEX, ACTION_ALTER);
        }
    }

    @Override
    protected String getStmtAction() {
        return alterIdxAllAction != null ? alterIdxAllAction
                : getStrForStmtAction(ACTION_ALTER, DbObjType.INDEX,
                getIdentifiers(ctx.schema_qualified_name()));
    }
}
