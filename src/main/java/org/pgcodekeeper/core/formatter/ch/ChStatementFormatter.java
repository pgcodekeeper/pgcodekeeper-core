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

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.pgcodekeeper.core.formatter.FormatConfiguration;
import org.pgcodekeeper.core.formatter.FormatParseTreeListener;
import org.pgcodekeeper.core.formatter.IndentDirection;
import org.pgcodekeeper.core.formatter.StatementFormatter;
import org.pgcodekeeper.core.parsers.antlr.CodeUnitToken;
import org.pgcodekeeper.core.parsers.antlr.generated.CHLexer;
import org.pgcodekeeper.core.parsers.antlr.generated.CHParser.Select_stmtContext;
import org.pgcodekeeper.core.utils.Pair;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * ClickHouse-specific implementation of SQL statement formatting.
 * Handles formatting of ClickHouse SELECT statements with dialect-specific rules.
 */
public class ChStatementFormatter extends StatementFormatter {

    private final List<? extends Token> tokens;

    /**
     * Constructs a new ClickHouse statement formatter for a SELECT statement.
     *
     * @param start         The starting offset in the source text (inclusive)
     * @param stop          The ending offset in the source text (exclusive)
     * @param selectStmtCtx The ANTLR parse tree context for the SELECT statement
     * @param tokenStream   The token stream containing all tokens
     * @param config        The formatting configuration options
     */
    public ChStatementFormatter(int start, int stop, Select_stmtContext selectStmtCtx,
            CommonTokenStream tokenStream, FormatConfiguration config) {
        super(start, stop, 0, 0, config);
        this.tokens = analyzeDefinition(selectStmtCtx, tokenStream);
        if (!tokens.isEmpty()) {
            lastTokenOffset = ((CodeUnitToken) tokens.get(0)).getCodeUnitStart();
        }
    }

    @Override
    public List<? extends Token> getTokens() {
        return tokens;
    }

    @Override
    protected FormatParseTreeListener getListener(CommonTokenStream tokenStream,
            Map<Token, Pair<IndentDirection, Integer>> indents, Set<Token> unaryOps) {
        return new ChFormatParseTreeListener(tokenStream, indents, unaryOps);
    }

    @Override
    protected boolean isTabToken(int type) {
        return type == CHLexer.TAB;
    }

    @Override
    protected boolean isSpaceToken(int type) {
        return type == CHLexer.SPACE;
    }

    @Override
    protected boolean isNewLineToken(int type) {
        return type == CHLexer.NEW_LINE;
    }

    @Override
    protected boolean isOperatorToken(int type, Token t) {
        return switch (type) {
            case CHLexer.EQ_DOUBLE, CHLexer.EQ_SINGLE, CHLexer.NOT_EQ, CHLexer.LE, CHLexer.GE, CHLexer.LT, CHLexer.GT,
                 CHLexer.CONCAT, CHLexer.PLUS, CHLexer.MINUS, CHLexer.ASTERISK, CHLexer.SLASH, CHLexer.PERCENT,
                 CHLexer.AND, CHLexer.OR, CHLexer.NOT_DIST, CHLexer.MOD, CHLexer.DIV -> true;
            default -> false;
        };
    }
}