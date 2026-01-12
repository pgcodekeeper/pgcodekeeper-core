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
import org.pgcodekeeper.core.DangerStatement;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.Alter_sequenceContext;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.Qualified_nameContext;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.Schema_alterContext;
import org.pgcodekeeper.core.database.api.schema.ObjectLocation;
import org.pgcodekeeper.core.database.ms.schema.MsDatabase;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.Arrays;
import java.util.Collections;

/**
 * Parser for Microsoft SQL ALTER statements that handle schemas, users, and sequences.
 * Processes ALTER SCHEMA, ALTER USER, and ALTER SEQUENCE statements with appropriate
 * danger warnings for potentially destructive operations like RESTART WITH.
 */
public final class AlterMsOther extends MsParserAbstract {

    private final Schema_alterContext ctx;

    /**
     * Creates a parser for Microsoft SQL ALTER statements.
     *
     * @param ctx      the ANTLR parse tree context for the ALTER statement
     * @param db       the Microsoft SQL database schema being processed
     * @param settings parsing configuration settings
     */
    public AlterMsOther(Schema_alterContext ctx, MsDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        if (ctx.alter_schema_sql() != null) {
            addObjReference(Collections.singletonList(ctx.alter_schema_sql().schema_name),
                    DbObjType.SCHEMA, ACTION_ALTER);
        } else if (ctx.alter_user() != null) {
            addObjReference(Collections.singletonList(ctx.alter_user().username), DbObjType.USER, ACTION_ALTER);
        } else if (ctx.alter_sequence() != null) {
            alterSequence(ctx.alter_sequence());
        }
    }

    private void alterSequence(Alter_sequenceContext alter) {
        Qualified_nameContext qname = alter.qualified_name();
        ObjectLocation ref = addObjReference(Arrays.asList(qname.schema, qname.name),
                DbObjType.SEQUENCE, ACTION_ALTER);
        if (!alter.RESTART().isEmpty()) {
            ref.setWarning(DangerStatement.RESTART_WITH);
        }
    }

    @Override
    protected ObjectLocation fillQueryLocation(ParserRuleContext ctx) {
        ObjectLocation loc = super.fillQueryLocation(ctx);
        Alter_sequenceContext alterSeqCtx = ((Schema_alterContext) ctx).alter_sequence();
        if (alterSeqCtx != null && !alterSeqCtx.RESTART().isEmpty()) {
            loc.setWarning(DangerStatement.RESTART_WITH);
        }
        return loc;
    }

    @Override
    protected String getStmtAction() {
        if (ctx.alter_schema_sql() != null) {
            return getStrForStmtAction(ACTION_ALTER, DbObjType.SCHEMA, ctx.alter_schema_sql().schema_name);
        }
        if (ctx.alter_user() != null) {
            return getStrForStmtAction(ACTION_ALTER, DbObjType.USER, ctx.alter_user().username);
        }
        if (ctx.alter_sequence() != null) {
            return getStrForStmtAction(ACTION_ALTER, DbObjType.SEQUENCE, ctx.alter_sequence().qualified_name());
        }
        return null;
    }
}