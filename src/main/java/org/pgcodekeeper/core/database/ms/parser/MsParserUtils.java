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
package org.pgcodekeeper.core.database.ms.parser;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Queue;

import org.antlr.v4.runtime.*;
import org.pgcodekeeper.core.database.base.parser.*;
import org.pgcodekeeper.core.database.ms.parser.generated.TSQLLexer;
import org.pgcodekeeper.core.database.ms.parser.generated.TSQLParser;
import org.pgcodekeeper.core.exception.MonitorCancelledRuntimeException;
import org.pgcodekeeper.core.exception.UnresolvedReferenceException;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.monitor.NullMonitor;
import org.pgcodekeeper.core.settings.DiffSettings;
import org.pgcodekeeper.core.utils.InputStreamProvider;
import org.pgcodekeeper.core.utils.Pair;

/**
 * Utility methods for Microsoft SQL parsing with ANTLR.
 * <p>
 * Provides helper functions for working with Microsoft SQL syntax
 * during ANTLR-based parsing
 * </p>
 */
public final class MsParserUtils {

    private static volatile long msParserLastStart;

    /**
     * Creates a Microsoft SQL parser from string input.
     *
     * @param sql              T-SQL string to parse
     * @param parsedObjectName name of the object being parsed (for error reporting)
     * @param errors           list to collect parsing errors
     * @return configured TSQLParser instance
     */
    public static TSQLParser createSqlParser(String sql, String parsedObjectName, List<Object> errors) {
        var stream = CharStreams.fromString(sql);
        return createSqlParser(stream, parsedObjectName, errors);
    }

    /**
     * Creates a Microsoft SQL parser from input stream.
     *
     * @param is               input stream containing T-SQL
     * @param charset          character encoding of the stream
     * @param parsedObjectName name of the object being parsed (for error reporting)
     * @param errors           list to collect parsing errors
     * @return configured TSQLParser instance
     * @throws IOException if there's an error reading the stream
     */
    public static TSQLParser createSqlParser(InputStream is, String charset, String parsedObjectName,
                                              List<Object> errors) throws IOException {
        var stream = CharStreams.fromStream(is, Charset.forName(charset));
        return createSqlParser(stream, parsedObjectName, errors);
    }

    private static TSQLParser createSqlParser(CharStream stream, String parsedObjectName, List<Object> errors) {
        TSQLLexer lexer = new TSQLLexer(stream);
        TSQLParser parser = new TSQLParser(new CommonTokenStream(lexer));
        AntlrParser.addErrorListener(lexer, parser, parsedObjectName, errors, 0, 0, 0);
        parser.setErrorHandler(new MsCustomAntlrErrorStrategy());
        msParserLastStart = System.currentTimeMillis();
        return parser;
    }

    /**
     * Parses Microsoft SQL stream asynchronously.
     *
     * @param inputStream      provider of the input stream
     * @param parsedObjectName name of the object being parsed
     * @param diffSettings     unified context object containing settings, monitor, and error accumulator
     * @param monitoringLevel  level of parse tree monitoring
     * @param listener         processor for the parsed content
     * @param antlrTasks       queue for parser tasks
     */
    public static void parseSqlStream(InputStreamProvider inputStream, String parsedObjectName,
                                       DiffSettings diffSettings, int monitoringLevel,
                                       IMsContextProcessor listener, Queue<AntlrTask<?>> antlrTasks) {
        List<Object> errors = diffSettings.getErrors();
        IMonitor mon = diffSettings.getMonitor();
        String charsetName = diffSettings.getSettings().getInCharsetName();
        AntlrTaskManager.submit(antlrTasks, () -> {
            IMonitor.checkCancelled(mon);
            try (InputStream stream = inputStream.getStream()) {
                var parser = createSqlParser(stream, charsetName, parsedObjectName, errors);
                parser.addParseListener(new CustomParseTreeListener(
                        monitoringLevel, mon == null ? new NullMonitor() : mon));
                return new Pair<>((CommonTokenStream) parser.getInputStream(), parser.tsql_file());
            } catch (MonitorCancelledRuntimeException mcre) {
                throw new InterruptedException();
            }
        }, pair -> {
            try {
                listener.process(pair.getSecond(), pair.getFirst());
            } catch (UnresolvedReferenceException ex) {
                errors.add(CustomParserListener.handleUnresolvedReference(ex, parsedObjectName));
            }
        });
    }

    /**
     * Checks if parser caches need cleaning based on last usage time.
     *
     * @param cleaningInterval time interval in milliseconds after which cache should be cleaned
     */
    public static void checkToClean(long cleaningInterval) {
        checkToClean(cleaningInterval, msParserLastStart);
    }

    private static void checkToClean(long cleaningInterval, long parserLastStart) {
        if (parserLastStart != 0 && (cleaningInterval < System.currentTimeMillis() - parserLastStart)) {
            cleanParserCache();
        }
    }

    /**
     * Clears the MS SQL parser cache.
     */
    // new method for cleanCacheOfAllParsers()
    public static void cleanCacheMsParser() {
        if (msParserLastStart != 0) {
            cleanParserCache();
        }
    }

    protected static void cleanParserCache() {
        Parser parser = createSqlParser(AntlrParser.SQL, AntlrParser.PARSED_OBJ_NAME, null);
        msParserLastStart = 0;
        parser.getInterpreter().clearDFA();
    }

    private MsParserUtils() {
    }
}
