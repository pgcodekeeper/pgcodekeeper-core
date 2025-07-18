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
package org.pgcodekeeper.core.formatter.pg;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.pgcodekeeper.core.formatter.ErrorPresenceListener;
import org.pgcodekeeper.core.formatter.FormatConfiguration;
import org.pgcodekeeper.core.formatter.FormatParseTreeListener;
import org.pgcodekeeper.core.formatter.IndentDirection;
import org.pgcodekeeper.core.formatter.StatementFormatter;
import org.pgcodekeeper.core.parsers.antlr.AntlrUtils;
import org.pgcodekeeper.core.parsers.antlr.CodeUnitToken;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLLexer;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.Function_bodyContext;
import org.pgcodekeeper.core.utils.Pair;

public class PgStatementFormatter extends StatementFormatter {

    private List<? extends Token> tokens;

    public PgStatementFormatter(int start, int stop, String functionDefinition,
            int defOffset, String language, FormatConfiguration config) {
        super(start, stop, defOffset, defOffset, config);
        this.tokens = analyzeDefinition(functionDefinition, language);
    }

    public PgStatementFormatter(int start, int stop, Function_bodyContext definition,
            CommonTokenStream tokenStream, FormatConfiguration config) {
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
}