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

import java.util.Arrays;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.DangerStatement;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.generated.TSQLParser.Alter_sequenceContext;
import org.pgcodekeeper.core.parsers.antlr.generated.TSQLParser.Qualified_nameContext;
import org.pgcodekeeper.core.parsers.antlr.generated.TSQLParser.Schema_alterContext;
import org.pgcodekeeper.core.schema.PgObjLocation;
import org.pgcodekeeper.core.schema.ms.MsDatabase;
import org.pgcodekeeper.core.settings.ISettings;

public final class AlterMsOther extends MsParserAbstract {

    private final Schema_alterContext ctx;

    public AlterMsOther(Schema_alterContext ctx, MsDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        if (ctx.alter_schema_sql() != null) {
            addObjReference(Arrays.asList(ctx.alter_schema_sql().schema_name),
                    DbObjType.SCHEMA, ACTION_ALTER);
        } else if (ctx.alter_user() != null) {
            addObjReference(Arrays.asList(ctx.alter_user().username), DbObjType.USER, ACTION_ALTER);
        } else if (ctx.alter_sequence() != null) {
            alterSequence(ctx.alter_sequence());
        }
    }

    private void alterSequence(Alter_sequenceContext alter) {
        Qualified_nameContext qname = alter.qualified_name();
        PgObjLocation ref = addObjReference(Arrays.asList(qname.schema, qname.name),
                DbObjType.SEQUENCE, ACTION_ALTER);
        if (!alter.RESTART().isEmpty()) {
            ref.setWarning(DangerStatement.RESTART_WITH);
        }
    }

    @Override
    protected PgObjLocation fillQueryLocation(ParserRuleContext ctx) {
        PgObjLocation loc = super.fillQueryLocation(ctx);
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