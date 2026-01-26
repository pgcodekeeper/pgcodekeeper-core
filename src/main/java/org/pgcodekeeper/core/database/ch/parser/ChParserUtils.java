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
package org.pgcodekeeper.core.database.ch.parser;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.*;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.Interval;
import org.pgcodekeeper.core.database.base.parser.*;
import org.pgcodekeeper.core.database.ch.parser.generated.CHLexer;
import org.pgcodekeeper.core.database.ch.parser.generated.CHParser;
import org.pgcodekeeper.core.database.ch.parser.statement.ChParserAbstract;
import org.pgcodekeeper.core.exception.MonitorCancelledRuntimeException;
import org.pgcodekeeper.core.exception.UnresolvedReferenceException;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.monitor.NullMonitor;
import org.pgcodekeeper.core.utils.InputStreamProvider;
import org.pgcodekeeper.core.utils.Pair;

/**
 * Utility methods for ClickHouse SQL parsing with ANTLR.
 * <p>
 * Provides helper functions for working with ClickHouse SQL syntax
 * during ANTLR-based parsing
 * </p>
 */
public final class ChParserUtils {

    private static volatile long chParserLastStart;

    /**
     * Creates a ClickHouse SQL parser from string input.
     *
     * @param sql              ClickHouse SQL string to parse
     * @param parsedObjectName name of the object being parsed (for error reporting)
     * @param errors           list to collect parsing errors
     * @return configured CHParser instance
     */
    public static CHParser createParser(String sql, String parsedObjectName, List<Object> errors) {
        var stream = CharStreams.fromString(sql);
        return createParser(stream, parsedObjectName, errors);
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
    public static CHParser createParser(InputStream is, String charset, String parsedObjectName,
                                          List<Object> errors) throws IOException {
        var stream = CharStreams.fromStream(is, Charset.forName(charset));
        return createParser(stream, parsedObjectName, errors);
    }

    private static CHParser createParser(CharStream stream, String parsedObjectName, List<Object> errors) {
        Lexer lexer = new CHLexer(stream);
        CHParser parser = new CHParser(new CommonTokenStream(lexer));
        AntlrParser.addErrorListener(lexer, parser, parsedObjectName, errors, 0, 0, 0);
        parser.setErrorHandler(new ChCustomAntlrErrorStrategy());
        chParserLastStart = System.currentTimeMillis();
        return parser;
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
    public static void parseSqlStream(InputStreamProvider inputStream, String charsetName, String parsedObjectName,
                                        List<Object> errors, IMonitor mon, int monitoringLevel, IChContextProcessor listener,
                                        Queue<AntlrTask<?>> antlrTasks) {
        AntlrTaskManager.submit(antlrTasks, () -> {
            IMonitor.checkCancelled(mon);
            try (InputStream stream = inputStream.getStream()) {
                var parser = createParser(stream, charsetName,
                        parsedObjectName, errors);
                parser.addParseListener(new CustomParseTreeListener(
                        monitoringLevel, mon == null ? new NullMonitor() : mon));
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
     * Checks if parser caches need cleaning based on last usage time.
     *
     * @param cleaningInterval time interval in milliseconds after which cache should be cleaned
     */
    public static void checkToCleanChParser(long cleaningInterval) {
        checkToClean(cleaningInterval, chParserLastStart);
    }

    private static void checkToClean(long cleaningInterval, long parserLastStart) {
        if (parserLastStart != 0 && (cleaningInterval < System.currentTimeMillis() - parserLastStart)) {
            cleanParserCache();
        }
    }

    /**
     * Clears the ClickHouse parser cache.
     */
    // new method for cleanCacheOfAllParsers()
    public static void cleanCacheChParser() {
        if (chParserLastStart != 0) {
            cleanParserCache();
        }
    }

    private static void cleanParserCache() {
        Parser parser = createParser(AntlrParser.SQL, AntlrParser.PARSED_OBJ_NAME, null);
        chParserLastStart = 0;
        parser.getInterpreter().clearDFA();
    }

    public static boolean isSpecialChar(int type, int previous) {
            return previous == CHLexer.DOT
                    || previous == CHLexer.LPAREN
                    || type == CHLexer.DOT
                    || type == CHLexer.RPAREN
                    || type == CHLexer.LPAREN
                    || type == CHLexer.COMMA;
    }

    public static String normalizeWhitespaceUnquoted(ParserRuleContext ctx, CommonTokenStream stream) {
        StringBuilder sb = new StringBuilder();

        // skip space before first token
        int previous = CHLexer.DOT;

        List<Token> tokens = stream.getTokens();
        for (int i = ctx.getStart().getTokenIndex(); i <= ctx.getStop().getTokenIndex(); i++) {
            Token token  = tokens.get(i);
            // skip tokens from non default channel
            if (token.getChannel() != Token.DEFAULT_CHANNEL) {
                continue;
            }
            int type = token.getType();

            // remove whitespace after and before some special characters for PG and CH
            if (!isSpecialChar(type, previous)) {
                sb.append(' ');
            }
            sb.append(getTokenText(type, token));
            previous = type;
        }
        return sb.toString();
    }

    public static String getTokenText(int type, Token token) {
            if (type == CHLexer.DOUBLE_QUOTED_IDENTIFIER
                    || type == CHLexer.BACK_QUOTED_IDENTIFIER) {
                // get text with quotes
                return token.getInputStream().getText(
                        Interval.of(token.getStartIndex(), token.getStopIndex()));
            }

            if (CHLexer.ALL <= type && type <= CHLexer.WITH) {
                // upper case reserved keywords
                return token.getText().toUpperCase(Locale.ROOT);
            }

        return token.getText();
    }

    /**
     * Parses a ClickHouse qualified name into its components.
     *
     * @param schemaQualifiedName the qualified name string to parse
     * @return QNameParser instance containing parsed components
     */
    public static QNameParser<ParserRuleContext> parseQName(String schemaQualifiedName) {
        List<Object> errors = new ArrayList<>();
        var parser = createParser(schemaQualifiedName, "qname: " + schemaQualifiedName, errors);
        var parts = ChParserAbstract.getIdentifiers(parser.qname_parser().qualified_name());
        return new QNameParser<>(parts, errors);
    }

    /**
     * Creates a wrapper for parsing ClickHouse qualified names.
     *
     * @param fullName the qualified name string to parse
     * @return wrapper containing parsed name components
     */
    public static QNameParserWrapper wrapParsedQName(String fullName) {
        return new QNameParserWrapper(parseQName(fullName));
    }

    private ChParserUtils() {
    }
}
