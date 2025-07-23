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
import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.parsers.antlr.generated.CHParser.Ch_fileContext;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.SqlContext;
import org.pgcodekeeper.core.parsers.antlr.generated.TSQLParser.Tsql_fileContext;

/**
 * Interface for processing ANTLR parser rule contexts with token streams
 *
 * @param <R> type of the parser rule context to be processed
 */
public interface AntlrContextProcessor<R extends ParserRuleContext> {
    /**
     * Processes the ANTLR parser rule context with the given token stream
     *
     * @param rootCtx the root parser rule context to process
     * @param stream  the token stream associated with the context
     */
    void process(R rootCtx, CommonTokenStream stream);

    /**
     * Processor for PostgreSQL contexts
     */
    interface SqlContextProcessor extends AntlrContextProcessor<SqlContext> {
    }

    /**
     * Processor for Microsoft SQL contexts
     */
    interface TSqlContextProcessor extends AntlrContextProcessor<Tsql_fileContext> {
    }

    /**
     * Processor for ClickHouse SQL contexts
     */
    interface ChSqlContextProcessor extends AntlrContextProcessor<Ch_fileContext> {
    }
}
