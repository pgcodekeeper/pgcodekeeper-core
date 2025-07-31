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
package org.pgcodekeeper.core.parsers.antlr;

import org.antlr.v4.runtime.CommonTokenStream;
import org.pgcodekeeper.core.loader.ParserListenerMode;
import org.pgcodekeeper.core.parsers.antlr.AntlrContextProcessor.ChSqlContextProcessor;
import org.pgcodekeeper.core.parsers.antlr.generated.CHParser.Ch_fileContext;
import org.pgcodekeeper.core.parsers.antlr.generated.CHParser.Privilegy_stmtContext;
import org.pgcodekeeper.core.parsers.antlr.generated.CHParser.QueryContext;
import org.pgcodekeeper.core.parsers.antlr.statements.ch.GrantChPrivilege;
import org.pgcodekeeper.core.schema.PgStatement;
import org.pgcodekeeper.core.schema.StatementOverride;
import org.pgcodekeeper.core.schema.ch.ChDatabase;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.utils.IMonitor;

import java.util.List;
import java.util.Map;

/**
 * ANTLR listener for processing ClickHouse SQL statements with override support.
 * Handles privilege statements and applies statement overrides.
 */
public final class ChSQLOverridesListener extends CustomParserListener<ChDatabase> implements ChSqlContextProcessor {

    private final Map<PgStatement, StatementOverride> overrides;

    /**
     * Creates a new listener for ClickHouse SQL with override support.
     *
     * @param database  the target database schema
     * @param filename  name of the file being parsed
     * @param mode      parsing mode
     * @param errors    list to collect parsing errors
     * @param monitor   progress monitor for cancellation support
     * @param overrides map of statement overrides to apply
     * @param settings  application settings
     */
    public ChSQLOverridesListener(ChDatabase database, String filename, ParserListenerMode mode, List<Object> errors,
                                  IMonitor monitor, Map<PgStatement, StatementOverride> overrides, ISettings settings) {
        super(database, filename, mode, errors, monitor, settings);
        this.overrides = overrides;
    }

    /**
     * Processes the complete ClickHouse SQL file context.
     * Extracts and processes all queries in the file.
     *
     * @param rootCtx the root file context from ANTLR parser
     * @param stream  the token stream associated with the context
     */
    @Override
    public void process(Ch_fileContext rootCtx, CommonTokenStream stream) {
        for (QueryContext query : rootCtx.query()) {
            query(query);
        }
    }

    private void query(QueryContext query) {
        var ddlStmt = query.stmt().ddl_stmt();
        if (ddlStmt == null) {
            return;
        }

        Privilegy_stmtContext privilStmt = ddlStmt.privilegy_stmt();
        if (privilStmt != null) {
            safeParseStatement(new GrantChPrivilege(privilStmt, db, overrides, settings), ddlStmt);
        }
    }
}
