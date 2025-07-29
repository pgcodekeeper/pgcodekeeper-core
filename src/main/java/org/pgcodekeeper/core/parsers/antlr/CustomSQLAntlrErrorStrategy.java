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

import org.antlr.v4.runtime.Vocabulary;
import org.antlr.v4.runtime.misc.IntervalSet;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLLexer;

import java.util.stream.Stream;

/**
 * PostgreSQL-specific error strategy that provides enhanced error reporting for
 * PostgreSQL SQL parsing.
 */
class CustomSQLAntlrErrorStrategy extends CustomAntlrErrorStrategy {

    @Override
    protected Stream<Integer> getTokenStream(IntervalSet tokens) {
        Stream<Integer> stream = tokens.toList().stream();

        if (tokens.contains(SQLLexer.Identifier)) {
            stream = stream.filter(e -> e < SQLLexer.ABORT || SQLLexer.WHILE < e);
        }
        if (tokens.contains(SQLLexer.OP_CHARS)) {
            stream = stream.filter(e -> e < SQLLexer.CAST_EXPRESSION || SQLLexer.HASH_SIGN < e);
        }
        return stream;
    }

    @Override
    protected String getTokenName(Integer token, Vocabulary vocabulary) {
        return switch (token) {
            case SQLLexer.OP_CHARS -> OPERATOR;
            case SQLLexer.DOLLAR_NUMBER, SQLLexer.Identifier, SQLLexer.QuotedIdentifier,
                 SQLLexer.UnicodeQuotedIdentifier -> IDENTIFIER;
            case SQLLexer.StringConstant, SQLLexer.BeginDollarStringConstant, SQLLexer.UnicodeEscapeStringConstant ->
                    STRING;
            case SQLLexer.Integer, SQLLexer.Numeric -> NUMBER;
            default -> vocabulary.getDisplayName(token);
        };
    }
}
