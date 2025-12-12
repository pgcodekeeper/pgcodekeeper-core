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
package org.pgcodekeeper.core.parsers.antlr.ms.statement;

import org.antlr.v4.runtime.tree.ParseTree;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.Insert_statementContext;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.Qualified_nameContext;
import org.pgcodekeeper.core.database.ms.schema.MsDatabase;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.Arrays;

/**
 * Parser for Microsoft SQL INSERT statements.
 * Handles INSERT operations including table references and dependency tracking.
 */
public final class InsertMsStatement extends MsParserAbstract {

    private final Insert_statementContext ctx;

    /**
     * Creates a parser for Microsoft SQL INSERT statements.
     *
     * @param ctx      the ANTLR parse tree context for the INSERT statement
     * @param db       the Microsoft SQL database schema being processed
     * @param settings parsing configuration settings
     */
    public InsertMsStatement(Insert_statementContext ctx, MsDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        Qualified_nameContext qname = ctx.qualified_name();
        if (qname != null) {
            addObjReference(Arrays.asList(qname.schema, qname.name),
                    DbObjType.TABLE, ACTION_INSERT);
        }
    }

    @Override
    protected String getStmtAction() {
        ParseTree id = ctx.qualified_name();
        if (id == null) {
            id = ctx.rowset_function_limited();
        }
        if (id == null) {
            id = ctx.LOCAL_ID();
        }
        return getStrForStmtAction(ACTION_INSERT + " INTO", DbObjType.TABLE, id);
    }
}
