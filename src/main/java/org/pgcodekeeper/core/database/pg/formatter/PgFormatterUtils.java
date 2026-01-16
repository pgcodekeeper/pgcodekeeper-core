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
package org.pgcodekeeper.core.database.pg.formatter;

import java.util.List;

import org.antlr.v4.runtime.Token;
import org.pgcodekeeper.core.database.pg.parser.generated.SQLLexer;

/**
 * Utility class containing PostgreSQL-specific formatting helper methods.
 * Provides common operations for identifying and handling SQL operators.
 */
public class PgFormatterUtils {

    /**
     * Checks if a token type represents a SQL operator.
     *
     * @param type The token type to check (from SQLLexer)
     * @return true if the token type is a recognized operator, false otherwise
     */
    public static boolean isOperatorToken(int type) {
        return switch (type) {
            case SQLLexer.EQUAL, SQLLexer.NOT_EQUAL, SQLLexer.LTH, SQLLexer.LEQ, SQLLexer.GTH, SQLLexer.GEQ,
                    SQLLexer.PLUS, SQLLexer.MINUS, SQLLexer.MULTIPLY, SQLLexer.DIVIDE, SQLLexer.MODULAR, SQLLexer.EXP,
                    SQLLexer.EQUAL_GTH, SQLLexer.COLON_EQUAL, SQLLexer.LESS_LESS, SQLLexer.GREATER_GREATER,
                    SQLLexer.HASH_SIGN, SQLLexer.OP_CHARS -> true;
            default -> false;
        };
    }


    /**
     * Validates whether a token should be treated as an operator in its context.
     *
     * @param t      The token being checked
     * @param type   The token type (from SQLLexer)
     * @param tokens The complete list of tokens for context analysis
     * @return true if the token should be treated as an operator, false otherwise
     */
    public static boolean checkOperator(Token t, int type, List<? extends Token> tokens) {
        if (type == SQLLexer.MODULAR) {
            int nextTokenType = tokens.get(tokens.indexOf(t) + 1).getType();
            return nextTokenType != SQLLexer.ROWTYPE && nextTokenType != SQLLexer.TYPE;
        }

        if (type != SQLLexer.MULTIPLY) {
            return true;
        }

        int prevTokenType = tokens.get(tokens.indexOf(t) - 1).getType();
        int nextTokenType = tokens.get(tokens.indexOf(t) + 1).getType();
        return prevTokenType != SQLLexer.DOT
                && nextTokenType != SQLLexer.COMMA
                && prevTokenType != SQLLexer.LEFT_PAREN
                && nextTokenType != SQLLexer.RIGHT_PAREN;
    }

    private PgFormatterUtils() {
    }
}