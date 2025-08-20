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
package org.pgcodekeeper.core.formatter.ch;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Lexer;
import org.pgcodekeeper.core.formatter.AbstractFormatter;
import org.pgcodekeeper.core.formatter.FormatConfiguration;
import org.pgcodekeeper.core.formatter.FormatItem;
import org.pgcodekeeper.core.formatter.StatementFormatter;
import org.pgcodekeeper.core.parsers.antlr.base.CodeUnitToken;
import org.pgcodekeeper.core.parsers.antlr.ch.generated.CHLexer;
import org.pgcodekeeper.core.parsers.antlr.ch.generated.CHParser;
import org.pgcodekeeper.core.parsers.antlr.ch.generated.CHParser.Ddl_stmtContext;
import org.pgcodekeeper.core.parsers.antlr.ch.generated.CHParser.QueryContext;
import org.pgcodekeeper.core.parsers.antlr.ch.generated.CHParser.StmtContext;

import java.util.ArrayList;
import java.util.List;

/**
 * ClickHouse-specific SQL formatter implementation.
 * Handles formatting of ClickHouse queries and DDL statements with dialect-specific rules.
 */
public class ChFormatter extends AbstractFormatter {

    /**
     * Constructs a new ClickHouse formatter instance.
     *
     * @param source The source SQL text to format
     * @param start  The starting offset in the source text (inclusive)
     * @param stop   The ending offset in the source text (exclusive)
     * @param config The formatting configuration options
     */
    public ChFormatter(String source, int start, int stop, FormatConfiguration config) {
        super(source, start, stop, config);
    }


    /**
     * Gets the list of formatting changes to apply to the ClickHouse SQL text.
     * Parses the SQL and applies formatting rules to create view statements and their subqueries.
     *
     * @return List of FormatItem objects representing the formatting changes
     */
    @Override
    public List<FormatItem> getFormatItems() {
        List<FormatItem> changes = new ArrayList<>();

        Lexer lexer = new CHLexer(CharStreams.fromString(source));
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        CHParser parser = new CHParser(tokenStream);

        var rootCtx = parser.ch_file();
        for (QueryContext queryCtx : rootCtx.query()) {
            if (start <= ((CodeUnitToken) queryCtx.stop).getCodeUnitStop()
                    || ((CodeUnitToken) queryCtx.start).getCodeUnitStart() < stop) {
                fillChanges(queryCtx.stmt(), tokenStream, changes);
            }
        }

        return changes;
    }

    private void fillChanges(StmtContext stmtContext, CommonTokenStream tokenStream, List<FormatItem> changes) {
        Ddl_stmtContext ddlCtx = stmtContext.ddl_stmt();
        if (ddlCtx == null) {
            return;
        }

        var createCtx = ddlCtx.create_stmt();
        if (createCtx == null) {
            return;
        }

        var createViewCtx = createCtx.create_view_stmt();
        if (createViewCtx == null) {
            return;
        }

        var selectStmtCtx = createViewCtx.subquery_clause().select_stmt();
        if (selectStmtCtx == null) {
            return;
        }

        StatementFormatter sf = new ChStatementFormatter(start, stop, selectStmtCtx, tokenStream, config);
        sf.format();
        changes.addAll(sf.getChanges());
    }
}