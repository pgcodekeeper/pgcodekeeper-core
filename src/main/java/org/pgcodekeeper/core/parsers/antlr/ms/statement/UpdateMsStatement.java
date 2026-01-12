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
package org.pgcodekeeper.core.parsers.antlr.ms.statement;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.pgcodekeeper.core.DangerStatement;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.Qualified_nameContext;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.Update_statementContext;
import org.pgcodekeeper.core.database.api.schema.ObjectLocation;
import org.pgcodekeeper.core.database.ms.schema.MsDatabase;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.Arrays;

/**
 * Parser for Microsoft SQL UPDATE statements.
 * Handles UPDATE operations including table references, dependency tracking,
 * and danger statement warnings for potentially risky operations.
 */
public final class UpdateMsStatement extends MsParserAbstract {

    private final Update_statementContext ctx;

    /**
     * Creates a parser for Microsoft SQL UPDATE statements.
     *
     * @param ctx      the ANTLR parse tree context for the UPDATE statement
     * @param db       the Microsoft SQL database schema being processed
     * @param settings parsing configuration settings
     */
    public UpdateMsStatement(Update_statementContext ctx, MsDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        Qualified_nameContext qname = ctx.qualified_name();
        if (qname != null) {
            ObjectLocation loc = addObjReference(Arrays.asList(qname.schema, qname.name),
                    DbObjType.TABLE, ACTION_UPDATE);
            loc.setWarning(DangerStatement.UPDATE);
        }
    }

    @Override
    protected ObjectLocation fillQueryLocation(ParserRuleContext ctx) {
        ObjectLocation loc = super.fillQueryLocation(ctx);
        loc.setWarning(DangerStatement.UPDATE);
        return loc;
    }

    @Override
    protected String getStmtAction() {
        ParseTree id = ctx.qualified_name();
        if (id == null) {
            id = ctx.rowset_function_limited();
        }
        if (id == null) {
            id = ctx.LOCAL_ID(0);
        }
        return getStrForStmtAction(ACTION_UPDATE, DbObjType.TABLE, id);
    }
}
