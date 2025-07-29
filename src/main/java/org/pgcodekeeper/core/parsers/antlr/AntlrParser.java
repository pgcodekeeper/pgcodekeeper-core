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

import org.antlr.v4.runtime.*;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.core.runtime.NullProgressMonitor;
import org.pgcodekeeper.core.DatabaseType;
import org.pgcodekeeper.core.PgDiffUtils;
import org.pgcodekeeper.core.parsers.antlr.AntlrContextProcessor.ChSqlContextProcessor;
import org.pgcodekeeper.core.parsers.antlr.AntlrContextProcessor.SqlContextProcessor;
import org.pgcodekeeper.core.parsers.antlr.AntlrContextProcessor.TSqlContextProcessor;
import org.pgcodekeeper.core.parsers.antlr.exception.MonitorCancelledRuntimeException;
import org.pgcodekeeper.core.parsers.antlr.exception.UnresolvedReferenceException;
import org.pgcodekeeper.core.parsers.antlr.generated.*;
import org.pgcodekeeper.core.utils.InputStreamProvider;
import org.pgcodekeeper.core.utils.Pair;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.List;
import java.util.Queue;

/**
 * Utility class for creating and managing ANTLR parsers for different SQL dialects.
 * Provides methods for parsing SQL, Microsoft SQL, and ClickHouse SQL with error handling.
 */
public final class AntlrParser {

    private static volatile long pgParserLastStart;
    private static volatile long msParserLastStart;
    private static volatile long chParserLastStart;

    /**
     * Creates a parser for ignore list files.
     *
     * @param listFile path to the ignore list file
     * @return configured IgnoreListParser instance
     * @throws IOException if there's an error reading the file
     */
    public static IgnoreListParser createIgnoreListParser(Path listFile) throws IOException {
        String parsedObjectName = listFile.toString();
        var stream = CharStreams.fromPath(listFile);
        Lexer lexer = new IgnoreListLexer(stream);
        IgnoreListParser parser = new IgnoreListParser(new CommonTokenStream(lexer));
        addErrorListener(lexer, parser, parsedObjectName, null, 0, 0, 0);
        return parser;
    }

    /**
     * Creates a parser for PostgreSQL privilege strings.
     *
     * @param aclArrayAsString privilege string to parse
     * @return configured PrivilegesParser instance
     */
    public static PrivilegesParser createPrivilegesParser(String aclArrayAsString) {
        var stream = CharStreams.fromString(aclArrayAsString);
        Lexer lexer = new PrivilegesLexer(stream);
        PrivilegesParser parser = new PrivilegesParser(new CommonTokenStream(lexer));
        addErrorListener(lexer, parser, "jdbc privileges", null, 0, 0, 0);
        return parser;
    }

    /**
     * Creates a PostgreSQL SQL parser from string input.
     *
     * @param sql              SQL string to parse
     * @param parsedObjectName name of the object being parsed (for error reporting)
     * @param errors           list to collect parsing errors
     * @return configured SQLParser instance
     */
    public static SQLParser createSQLParser(String sql, String parsedObjectName, List<Object> errors) {
        var stream = CharStreams.fromString(sql);
        return createSQLParser(stream, parsedObjectName, errors, 0, 0, 0);
    }

    /**
     * Creates a PostgreSQL SQL parser from string input with position offset.
     *
     * @param sql              SQL string to parse
     * @param parsedObjectName name of the object being parsed (for error reporting)
     * @param errors           list to collect parsing errors
     * @param start            token providing position offset information
     * @return configured SQLParser instance
     */
    public static SQLParser createSQLParser(String sql, String parsedObjectName, List<Object> errors, Token start) {
        var stream = CharStreams.fromString(sql);
        CodeUnitToken cuCodeStart = (CodeUnitToken) start;
        int offset = cuCodeStart.getCodeUnitStart();
        int lineOffset = cuCodeStart.getLine() - 1;
        int inLineOffset = cuCodeStart.getCodeUnitPositionInLine();
        return createSQLParser(stream, parsedObjectName, errors, offset, lineOffset, inLineOffset);
    }

    /**
     * Creates a PostgreSQL SQL parser from input stream.
     *
     * @param is               input stream containing SQL
     * @param charset          character encoding of the stream
     * @param parsedObjectName name of the object being parsed (for error reporting)
     * @param errors           list to collect parsing errors
     * @return configured SQLParser instance
     * @throws IOException if there's an error reading the stream
     */
    public static SQLParser createSQLParser(InputStream is, String charset, String parsedObjectName,
                                            List<Object> errors) throws IOException {
        var stream = CharStreams.fromStream(is, Charset.forName(charset));
        return createSQLParser(stream, parsedObjectName, errors, 0, 0, 0);
    }

    private static SQLParser createSQLParser(CharStream stream, String parsedObjectName, List<Object> errors,
                                             int offset, int lineOffset, int inLineOffset) {
        SQLLexer lexer = new SQLLexer(stream);
        SQLParser parser = new SQLParser(new CommonTokenStream(lexer));
        addErrorListener(lexer, parser, parsedObjectName, errors, offset, lineOffset, inLineOffset);
        parser.setErrorHandler(new CustomSQLAntlrErrorStrategy());
        pgParserLastStart = System.currentTimeMillis();
        return parser;
    }

    /**
     * Creates a Microsoft SQL parser from string input.
     *
     * @param sql              T-SQL string to parse
     * @param parsedObjectName name of the object being parsed (for error reporting)
     * @param errors           list to collect parsing errors
     * @return configured TSQLParser instance
     */
    public static TSQLParser createTSQLParser(String sql, String parsedObjectName, List<Object> errors) {
        var stream = CharStreams.fromString(sql);
        return createTSQLParser(stream, parsedObjectName, errors);
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
    public static TSQLParser createTSQLParser(InputStream is, String charset, String parsedObjectName,
                                              List<Object> errors) throws IOException {
        var stream = CharStreams.fromStream(is, Charset.forName(charset));
        return createTSQLParser(stream, parsedObjectName, errors);
    }

    private static TSQLParser createTSQLParser(CharStream stream, String parsedObjectName, List<Object> errors) {
        TSQLLexer lexer = new TSQLLexer(stream);
        TSQLParser parser = new TSQLParser(new CommonTokenStream(lexer));
        addErrorListener(lexer, parser, parsedObjectName, errors, 0, 0, 0);
        parser.setErrorHandler(new CustomTSQLAntlrErrorStrategy());
        msParserLastStart = System.currentTimeMillis();
        return parser;
    }

    /**
     * Creates a ClickHouse SQL parser from string input.
     *
     * @param sql              ClickHouse SQL string to parse
     * @param parsedObjectName name of the object being parsed (for error reporting)
     * @param errors           list to collect parsing errors
     * @return configured CHParser instance
     */
    public static CHParser createCHParser(String sql, String parsedObjectName, List<Object> errors) {
        var stream = CharStreams.fromString(sql);
        return createCHParser(stream, parsedObjectName, errors);
    }

    /**
     * Creates a ClickHouse SQL parser from input stream.
     *
     * @param is               input stream containing ClickHouse SQL
     * @param charset          character encoding of the stream
     * @param parsedObjectName name of the object being parsed
     * @param errors           list to collect parsing errors
     * @return configured CHParser instance
     * @throws IOException if there's an error reading the stream
     */
    public static CHParser createCHParser(InputStream is, String charset, String parsedObjectName,
                                          List<Object> errors) throws IOException {
        var stream = CharStreams.fromStream(is, Charset.forName(charset));
        return createCHParser(stream, parsedObjectName, errors);
    }

    private static CHParser createCHParser(CharStream stream, String parsedObjectName, List<Object> errors) {
        Lexer lexer = new CHLexer(stream);
        CHParser parser = new CHParser(new CommonTokenStream(lexer));
        addErrorListener(lexer, parser, parsedObjectName, errors, 0, 0, 0);
        parser.setErrorHandler(new CustomChSQLAntlrErrorStrategy());
        chParserLastStart = System.currentTimeMillis();
        return parser;
    }

    private static void addErrorListener(Lexer lexer, Parser parser, String parsedObjectName,
                                         List<Object> errors, int offset, int lineOffset, int inLineOffset) {
        var listener = new CustomAntlrErrorListener(parsedObjectName, errors, offset, lineOffset, inLineOffset);
        lexer.removeErrorListeners();
        lexer.addErrorListener(listener);
        parser.removeErrorListeners();
        parser.addErrorListener(listener);
    }

    /**
     * Parses PostgreSQL SQL stream asynchronously.
     *
     * @param inputStream      provider of the input stream
     * @param charsetName      character encoding of the stream
     * @param parsedObjectName name of the object being parsed
     * @param errors           list to collect parsing errors
     * @param mon              progress monitor for cancellation support
     * @param monitoringLevel  level of parse tree monitoring
     * @param listener         processor for the parsed content
     * @param antlrTasks       queue for parser tasks
     */
    public static void parseSqlStream(InputStreamProvider inputStream, String charsetName,
                                      String parsedObjectName, List<Object> errors, IProgressMonitor mon, int monitoringLevel,
                                      SqlContextProcessor listener, Queue<AntlrTask<?>> antlrTasks) {
        AntlrTaskManager.submit(antlrTasks, () -> {
            PgDiffUtils.checkCancelled(mon);
            try (InputStream stream = inputStream.getStream()) {
                var parser = createSQLParser(stream, charsetName, parsedObjectName, errors);
                parser.addParseListener(new CustomParseTreeListener(
                        monitoringLevel, mon == null ? new NullProgressMonitor() : mon));
                return new Pair<>(parser.sql(), (CommonTokenStream) parser.getTokenStream());
            } catch (MonitorCancelledRuntimeException mcre) {
                throw new InterruptedException();
            }
        }, pair -> {
            try {
                listener.process(pair.getFirst(), pair.getSecond());
            } catch (UnresolvedReferenceException ex) {
                errors.add(CustomParserListener.handleUnresolvedReference(ex, parsedObjectName));
            }
        });
    }

    /**
     * Parses Microsoft SQL stream asynchronously.
     *
     * @param inputStream      provider of the input stream
     * @param charsetName      character encoding of the stream
     * @param parsedObjectName name of the object being parsed
     * @param errors           list to collect parsing errors
     * @param mon              progress monitor for cancellation support
     * @param monitoringLevel  level of parse tree monitoring
     * @param listener         processor for the parsed content
     * @param antlrTasks       queue for parser tasks
     */
    public static void parseTSqlStream(InputStreamProvider inputStream, String charsetName,
                                       String parsedObjectName, List<Object> errors, IProgressMonitor mon, int monitoringLevel,
                                       TSqlContextProcessor listener, Queue<AntlrTask<?>> antlrTasks) {
        AntlrTaskManager.submit(antlrTasks, () -> {
            PgDiffUtils.checkCancelled(mon);
            try (InputStream stream = inputStream.getStream()) {
                var parser = createTSQLParser(stream, charsetName, parsedObjectName, errors);
                parser.addParseListener(new CustomParseTreeListener(
                        monitoringLevel, mon == null ? new NullProgressMonitor() : mon));
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
     * Parses ClickHouse SQL stream asynchronously.
     *
     * @param inputStream      provider of the input stream
     * @param charsetName      character encoding of the stream
     * @param parsedObjectName name of the object being parsed
     * @param errors           list to collect parsing errors
     * @param mon              progress monitor for cancellation support
     * @param monitoringLevel  level of parse tree monitoring
     * @param listener         processor for the parsed content
     * @param antlrTasks       queue for parser tasks
     */
    public static void parseChSqlStream(InputStreamProvider inputStream, String charsetName, String parsedObjectName,
                                        List<Object> errors, IProgressMonitor mon, int monitoringLevel, ChSqlContextProcessor listener,
                                        Queue<AntlrTask<?>> antlrTasks) {
        AntlrTaskManager.submit(antlrTasks, () -> {
            PgDiffUtils.checkCancelled(mon);
            try (InputStream stream = inputStream.getStream()) {
                var parser = createCHParser(stream, charsetName, parsedObjectName, errors);
                parser.addParseListener(new CustomParseTreeListener(
                        monitoringLevel, mon == null ? new NullProgressMonitor() : mon));
                return new Pair<>((CommonTokenStream) parser.getInputStream(), parser.ch_file());
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
     * Clears the parser cache for all database types that have been used.
     */
    public static void cleanCacheOfAllParsers() {
        if (pgParserLastStart != 0) {
            cleanParserCache(DatabaseType.PG);
        }
        if (msParserLastStart != 0) {
            cleanParserCache(DatabaseType.MS);
        }
        if (chParserLastStart != 0) {
            cleanParserCache(DatabaseType.CH);
        }
    }

    /**
     * Checks if parser caches need cleaning based on last usage time.
     *
     * @param cleaningInterval time interval in milliseconds after which cache should be cleaned
     */
    public static void checkToClean(long cleaningInterval) {
        checkToClean(cleaningInterval, chParserLastStart, DatabaseType.CH);
        checkToClean(cleaningInterval, msParserLastStart, DatabaseType.MS);
        checkToClean(cleaningInterval, pgParserLastStart, DatabaseType.PG);
    }

    private static void checkToClean(long cleaningInterval, long parserLastStart, DatabaseType dbType) {
        if (parserLastStart != 0 && (cleaningInterval < System.currentTimeMillis() - parserLastStart)) {
            cleanParserCache(dbType);
        }
    }

    private static void cleanParserCache(DatabaseType databaseType) {
        String sql = ";";
        String parsedObjectName = "fake string to clean parser cache";
        Parser parser = switch (databaseType) {
            case CH -> {
                var chParser = createCHParser(sql, parsedObjectName, null);
                chParserLastStart = 0;
                yield chParser;
            }
            case MS -> {
                var msParser = createTSQLParser(sql, parsedObjectName, null);
                msParserLastStart = 0;
                yield msParser;
            }
            case PG -> {
                var pgParser = createSQLParser(sql, parsedObjectName, null);
                pgParserLastStart = 0;
                yield pgParser;
            }
        };
        parser.getInterpreter().clearDFA();
    }

    private AntlrParser() {
    }
}
