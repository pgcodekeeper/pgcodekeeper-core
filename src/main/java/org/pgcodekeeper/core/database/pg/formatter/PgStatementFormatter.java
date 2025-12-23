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
package org.pgcodekeeper.core.database.pg.formatter;

import org.antlr.v4.runtime.*;
import org.pgcodekeeper.core.database.api.formatter.IFormatConfiguration;
import org.pgcodekeeper.core.database.base.formatter.*;
import org.pgcodekeeper.core.parsers.antlr.base.AntlrUtils;
import org.pgcodekeeper.core.parsers.antlr.base.CodeUnitToken;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLLexer;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.Function_bodyContext;
import org.pgcodekeeper.core.utils.Pair;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * PostgreSQL-specific implementation of SQL statement formatting.
 * Handles formatting of PL/pgSQL functions and SQL statements with PostgreSQL-specific rules.
 */
public class PgStatementFormatter extends StatementFormatter {

    private final List<? extends Token> tokens;

    /**
     * Constructs a formatter for PostgreSQL functions from string definition.
     *
     * @param start              The starting offset in the source text
     * @param stop               The ending offset in the source text
     * @param functionDefinition The function definition text to format
     * @param defOffset          The function definition's offset in the source
     * @param language           The language of the function ('SQL' or 'plpgsql')
     * @param config             The formatting configuration
     */
    public PgStatementFormatter(int start, int stop, String functionDefinition,
            int defOffset, String language, IFormatConfiguration config) {
        super(start, stop, defOffset, defOffset, config);
        this.tokens = analyzeDefinition(functionDefinition, language);
    }

    /**
     * Constructs a formatter for PostgreSQL functions from parsed context.
     *
     * @param start       The starting offset in the source text
     * @param stop        The ending offset in the source text
     * @param definition  The parsed function body context
     * @param tokenStream The token stream containing all tokens
     * @param config      The formatting configuration
     */
    public PgStatementFormatter(int start, int stop, Function_bodyContext definition,
            CommonTokenStream tokenStream, IFormatConfiguration config) {
        super(start, stop, 0, 0, config);
        this.tokens = analyzeDefinition(definition, tokenStream);
        if (!tokens.isEmpty()) {
            lastTokenOffset = ((CodeUnitToken) tokens.get(0)).getCodeUnitStart();
        }
    }

    @Override
    public List<? extends Token> getTokens() {
        return tokens;
    }

    private List<? extends Token> analyzeDefinition(String definition, String language) {
        Lexer lexer = new SQLLexer(CharStreams.fromString(definition));
        if (isLexerOnly()) {
            // fast-path when no parser is required for advanced structure detection
            return lexer.getAllTokens();
        }
        CommonTokenStream stream = new CommonTokenStream(lexer);
        SQLParser parser = new SQLParser(stream);
        ErrorPresenceListener errorListener = new ErrorPresenceListener();
        parser.addErrorListener(errorListener);

        ParserRuleContext ctx;
        if ("SQL".equalsIgnoreCase(language)) {
            ctx = parser.sql();
            currentIndent = 0;
        } else {
            AntlrUtils.removeIntoStatements(parser);
            ctx = parser.plpgsql_function();
        }

        if (errorListener.isHasError()) {
            return lexer.getAllTokens();
        }

        runFormatListener(ctx, stream);
        return stream.getTokens();
    }

    /**
     * Creates a PostgreSQL-specific parse tree listener for formatting.
     *
     * @param tokenStream The token stream being processed
     * @param indents     Map to store indentation information
     * @param unaryOps    Set to track unary operators
     * @return PostgreSQL format listener instance
     */
    @Override
    protected FormatParseTreeListener getListener(CommonTokenStream tokenStream,
                                                  Map<Token, Pair<IndentDirection, Integer>> indents, Set<Token> unaryOps) {
        return new PgFormatParseTreeListener(tokenStream, indents, unaryOps);
    }

    @Override
    protected boolean isTabToken(int type) {
        return type == SQLLexer.Tab;
    }

    @Override
    protected boolean isSpaceToken(int type) {
        return type == SQLLexer.Space;
    }

    @Override
    protected boolean isNewLineToken(int type) {
        return type == SQLLexer.NewLine;
    }

    @Override
    protected boolean isOperatorToken(int type, Token t) {
        return PgFormatterUtils.isOperatorToken(type)
                && PgFormatterUtils.checkOperator(t, type, tokens);
    }

    /**
     * This listener checks if there was at least one syntax error while parsing.
     */
    private static final class ErrorPresenceListener extends BaseErrorListener {

        private boolean hasError;

        /**
         * @return true if there was an error, false otherwise.
         */
        public boolean isHasError() {
            return hasError;
        }

        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine,
                                String msg, RecognitionException e) {
            hasError = true;
        }
    }
}