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
package org.pgcodekeeper.core.parsers.antlr.base;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Custom error listener for ANTLR parsing that collects and processes syntax errors.
 * Handles error position adjustments and logging of parsing errors.
 */
final class CustomAntlrErrorListener extends BaseErrorListener {

    private static final Logger LOG = LoggerFactory.getLogger(CustomAntlrErrorListener.class);

    private final String parsedObjectName;
    private final List<Object> errors;
    private final int offset;
    private final int lineOffset;
    private final int inLineOffset;

    /**
     * Creates a new error listener with position adjustment parameters.
     *
     * @param parsedObjectName name of the object being parsed
     * @param errors           list to collect parsing errors
     * @param offset           character offset to apply to error positions
     * @param lineOffset       line number offset to apply
     * @param inLineOffset     in-line character position offset to apply
     */
    CustomAntlrErrorListener(String parsedObjectName, List<Object> errors,
                             int offset, int lineOffset, int inLineOffset) {
        this.parsedObjectName = parsedObjectName;
        this.errors = errors;
        this.offset = offset;
        this.lineOffset = lineOffset;
        this.inLineOffset = inLineOffset;
    }

    /**
     * Handles syntax errors detected during parsing.
     * Adjusts error positions, logs the error, and collects it if errors list was provided.
     *
     * @param recognizer         the parser/lexer that detected the error
     * @param offendingSymbol    the offending token/symbol
     * @param line               the line where error occurred
     * @param charPositionInLine the character position in line
     * @param msg                the error message
     * @param e                  the recognition exception
     */
    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
                            int line, int charPositionInLine, String msg, RecognitionException e) {
        Token token = offendingSymbol instanceof Token t ? t : null;
        AntlrError error = new AntlrError(token, parsedObjectName, line, charPositionInLine, msg)
                .copyWithOffset(offset, lineOffset, inLineOffset);

        var warningMsg = "ANTLR Error:\n%s".formatted(error);
        LOG.warn(warningMsg);
        if (errors != null) {
            errors.add(error);
        }
    }
}