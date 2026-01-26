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
package org.pgcodekeeper.core.database.pg.parser;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.*;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.Interval;
import org.pgcodekeeper.core.database.base.parser.*;
import org.pgcodekeeper.core.database.pg.parser.generated.*;
import org.pgcodekeeper.core.database.pg.parser.statement.PgParserAbstract;
import org.pgcodekeeper.core.exception.MonitorCancelledRuntimeException;
import org.pgcodekeeper.core.exception.UnresolvedReferenceException;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.monitor.NullMonitor;
import org.pgcodekeeper.core.utils.InputStreamProvider;
import org.pgcodekeeper.core.utils.Pair;

/**
 * Utility methods for PostgreSQL parsing with ANTLR.
 * <p>
 * Provides helper functions for working with PostgreSQL syntax
 * during ANTLR-based parsing
 * </p>
 */
public final class PgParserUtils {

    private static volatile long pgParserLastStart;

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
        AntlrParser.addErrorListener(lexer, parser, "jdbc privileges", null, 0, 0, 0);
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
    public static SQLParser createSqlParser(String sql, String parsedObjectName, List<Object> errors) {
        var stream = CharStreams.fromString(sql);
        return createSqlParser(stream, parsedObjectName, errors, 0, 0, 0);
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
    public static SQLParser createSqlParser(String sql, String parsedObjectName, List<Object> errors, Token start) {
        var stream = CharStreams.fromString(sql);
        CodeUnitToken cuCodeStart = (CodeUnitToken) start;
        int offset = cuCodeStart.getCodeUnitStart();
        int lineOffset = cuCodeStart.getLine() - 1;
        int inLineOffset = cuCodeStart.getCodeUnitPositionInLine();
        return createSqlParser(stream, parsedObjectName, errors, offset, lineOffset, inLineOffset);
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
    public static SQLParser createSqlParser(InputStream is, String charset, String parsedObjectName,
                                            List<Object> errors) throws IOException {
        var stream = CharStreams.fromStream(is, Charset.forName(charset));
        return createSqlParser(stream, parsedObjectName, errors, 0, 0, 0);
    }

    private static SQLParser createSqlParser(CharStream stream, String parsedObjectName, List<Object> errors,
                                             int offset, int lineOffset, int inLineOffset) {
        SQLLexer lexer = new SQLLexer(stream);
        SQLParser parser = new SQLParser(new CommonTokenStream(lexer));
        AntlrParser.addErrorListener(lexer, parser, parsedObjectName, errors, offset, lineOffset, inLineOffset);
        parser.setErrorHandler(new PgCustomAntlrErrorStrategy());
        pgParserLastStart = System.currentTimeMillis();
        return parser;
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
                                      String parsedObjectName, List<Object> errors, IMonitor mon, int monitoringLevel,
                                      IPgContextProcessor listener, Queue<AntlrTask<?>> antlrTasks) {
        AntlrTaskManager.submit(antlrTasks, () -> {
            IMonitor.checkCancelled(mon);
            try (InputStream stream = inputStream.getStream()) {
                var parser = createSqlParser(stream, charsetName, parsedObjectName, errors);
                parser.addParseListener(new CustomParseTreeListener(
                        monitoringLevel, mon == null ? new NullMonitor() : mon));
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
     * Checks if parser caches need cleaning based on last usage time.
     *
     * @param cleaningInterval time interval in milliseconds after which cache should be cleaned
     */
    public static void checkToClean(long cleaningInterval) {
        checkToClean(cleaningInterval, pgParserLastStart);
    }

    private static void checkToClean(long cleaningInterval, long parserLastStart) {
        if (parserLastStart != 0 && (cleaningInterval < System.currentTimeMillis() - parserLastStart)) {
            cleanParserCache();
        }
    }

    /**
     * Clears the PostgreSQL parser cache.
     */
    // new method for cleanCacheOfAllParsers()
    protected void cleanCachePgParser() {
        if (pgParserLastStart != 0) {
            cleanParserCache();
        }
    }

    protected static void cleanParserCache() {
        Parser parser = createSqlParser(AntlrParser.SQL, AntlrParser.PARSED_OBJ_NAME, null);
        pgParserLastStart = 0;
        parser.getInterpreter().clearDFA();
    }

    public static boolean isSpecialChar(int type, int previous) {
            return previous == SQLLexer.DOT
                    || previous == SQLLexer.LEFT_PAREN
                    || previous == SQLLexer.Text_between_Dollar
                    || previous == SQLLexer.BeginDollarStringConstant
                    || type == SQLLexer.DOT
                    || type == SQLLexer.RIGHT_PAREN
                    || type == SQLLexer.Text_between_Dollar
                    || type == SQLLexer.EndDollarStringConstant
                    || type == SQLLexer.COMMA;
    }

    public static String normalizeWhitespaceUnquoted(ParserRuleContext ctx, CommonTokenStream stream) {
        StringBuilder sb = new StringBuilder();

        // skip space before first token
        int previous = SQLLexer.DOT;

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
        if (type == SQLLexer.QuotedIdentifier
                // FIXME test this
                // || type == SQLLexer.UnicodeQuotedIdentifier
                // || type == SQLLexer.UnicodeEscapeStringConstant
                || type == SQLLexer.StringConstant) {
            // get text with quotes
            return token.getInputStream().getText(Interval.of(token.getStartIndex(), token.getStopIndex()));
        }

        if (SQLLexer.ALL <= type && type <= SQLLexer.WITH) {
            // upper case reserved keywords
            return token.getText().toUpperCase(Locale.ROOT);
        }

        return token.getText();
    }

    /**
     * Removes INTO statements from SQL tokens that aren't part of PL/pgSQL INTO clauses.
     * Handles special cases for INSERT INTO and IMPORT FOREIGN SCHEMA INTO.
     * <p>
     * Because INTO is sometimes used in the main SQL grammar, we have to be
     * careful not to take any such usage of INTO as a PL/pgSQL INTO clause.
     * There are currently three such cases:
     * <p>
     * 1. SELECT ... INTO.  We don't care, we just override that with the
     * PL/pgSQL definition.
     * <p>
     * 2. INSERT INTO.  This is relatively easy to recognize since the words
     * must appear adjacently; but we can't assume INSERT starts the command,
     * because it can appear in CREATE RULE or WITH.  Unfortunately, INSERT is
     * *not* fully reserved, so that means there is a chance of a false match,
     * but it's not very likely.
     * <p>
     * 3. IMPORT FOREIGN SCHEMA ... INTO.  This is not allowed in CREATE RULE
     * or WITH, so we just check for IMPORT as the command's first token.
     * (If IMPORT FOREIGN SCHEMA returned data someone might wish to capture
     * with an INTO-variables clause, we'd have to work much harder here.)
     * <p>
     * See <a href="https://github.com/postgres/postgres/blob/master/src/pl/plpgsql/src/pl_gram.y">pl_gram.y</a>
     */
    public static void removeIntoStatements(Parser parser) {
        CommonTokenStream stream = (CommonTokenStream) parser.getTokenStream();

        boolean isImport = false;
        int i = 0;

        while (true) {
            stream.seek(i++);
            int type = stream.LA(1);

            switch (type) {
            case Recognizer.EOF:
                stream.seek(0);
                parser.setInputStream(stream);
                return;
            case SQLLexer.SEMI_COLON:
                isImport = false;
                break;
            case SQLLexer.IMPORT:
                if (stream.LA(2) == SQLLexer.FOREIGN && stream.LA(3) == SQLLexer.SCHEMA) {
                    isImport = true;
                }
                break;
            case SQLLexer.INTO:
                if (isImport || stream.LA(-1) == SQLLexer.INSERT
                || stream.LA(-1) == SQLLexer.MERGE) {
                    break;
                }
                hideIntoTokens(stream);
                break;
            default:
                break;
            }
        }
    }

    private static void hideIntoTokens(CommonTokenStream stream) {
        int i = 1;
        int nextType = stream.LA(++i); // into

        if (nextType == SQLLexer.STRICT) {
            nextType = stream.LA(++i); // strict
        }

        if (isIdentifier(nextType)) {
            nextType = stream.LA(++i); // identifier

            while ((nextType == SQLLexer.DOT || nextType == SQLLexer.COMMA)
                    && isIdentifier(stream.LA(i + 1))) {
                i += 2; // comma or dot + identifier
                nextType = stream.LA(i);
            }

            // hide from end, because LT(p) skips hidden tokens
            for (int p = i - 1; p > 0; p--) {
                ((CommonToken) stream.LT(p)).setChannel(Token.HIDDEN_CHANNEL);
            }
        }
    }

    private static boolean isIdentifier(int type) {
        return SQLLexer.ABORT <= type && type <= SQLLexer.WHILE
                || type == SQLLexer.Identifier || type == SQLLexer.QuotedIdentifier;
    }

    /**
     * Parses a PostgreSQL qualified name into its components.
     *
     * @param schemaQualifiedName the qualified name string to parse
     * @return QNameParser instance containing parsed components
     */
    public static QNameParser<ParserRuleContext> parseQName(String schemaQualifiedName) {
        List<Object> errors = new ArrayList<>();
        var parser = PgParserUtils.createSqlParser(schemaQualifiedName, "qname: " + schemaQualifiedName, errors);
        var parts = PgParserAbstract.getIdentifiers(parser.qname_parser().schema_qualified_name());
        return new QNameParser<>(parts, errors);
    }

    /**
     * Parses a PostgreSQL operator name into its components.
     *
     * @param schemaQualifiedName the operator name string to parse
     * @return QNameParser instance containing parsed components
     */
    public static QNameParser<ParserRuleContext> parsePgOperator(String schemaQualifiedName) {
        List<Object> errors = new ArrayList<>();
        var parser = PgParserUtils.createSqlParser(schemaQualifiedName, "qname: " + schemaQualifiedName, errors);
        var parts = PgParserAbstract.getIdentifiers(parser.operator_args_parser().operator_name());
        return new QNameParser<>(parts, errors);
    }

    /**
     * Creates a wrapper for parsing PostgreSQL qualified names.
     *
     * @param fullName the qualified name string to parse (e.g. "schema.table")
     * @return wrapper containing parsed name components
     */
    public static QNameParserWrapper wrapParsedQName(String fullName) {
        return new QNameParserWrapper(parseQName(fullName));
    }

    /**
     * Creates a wrapper for parsing PostgreSQL operator names.
     *
     * @param fullName the operator name string to parse
     * @return wrapper containing parsed name components
     */
    public static QNameParserWrapper wrapParsedPgOperator(String fullName) {
        return new QNameParserWrapper(parsePgOperator(fullName));
    }

    private PgParserUtils() {
    }
}
