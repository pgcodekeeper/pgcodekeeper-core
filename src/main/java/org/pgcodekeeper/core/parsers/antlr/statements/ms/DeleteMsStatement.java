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

import org.antlr.v4.runtime.tree.ParseTree;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.generated.TSQLParser.Delete_statementContext;
import org.pgcodekeeper.core.parsers.antlr.generated.TSQLParser.Qualified_nameContext;
import org.pgcodekeeper.core.schema.ms.MsDatabase;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.Arrays;

/**
 * Parser for Microsoft SQL DELETE statements.
 * Handles DELETE operations including table references and dependency tracking.
 */
public final class DeleteMsStatement extends MsParserAbstract {

    private final Delete_statementContext ctx;

    /**
     * Creates a parser for Microsoft SQL DELETE statements.
     *
     * @param ctx      the ANTLR parse tree context for the DELETE statement
     * @param db       the Microsoft SQL database schema being processed
     * @param settings parsing configuration settings
     */
    public DeleteMsStatement(Delete_statementContext ctx, MsDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        Qualified_nameContext qname = ctx.qualified_name();
        if (qname != null) {
            addObjReference(Arrays.asList(qname.schema, qname.name),
                    DbObjType.TABLE, ACTION_DELETE);
        }
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
        return getStrForStmtAction(ACTION_DELETE + " FROM", DbObjType.TABLE, id);
    }
}
