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
package org.pgcodekeeper.core.parsers.antlr.base;

import org.antlr.v4.runtime.Token;
import org.pgcodekeeper.core.ContextLocation;

import java.io.Serial;

/**
 * Represents an error found during ANTLR parsing with location information
 */
public class AntlrError extends ContextLocation {

    @Serial
    private static final long serialVersionUID = 3133510141858228651L;

    private final String msg;
    private final String text;
    private final int stop;
    private final ErrorTypes errorType;

    /**
     * Creates an ANTLR error with default error type
     *
     * @param tokenError         the token where error occurred (may be null)
     * @param location           file path where error occurred
     * @param line               line number where error occurred (1-based)
     * @param charPositionInLine character position in line (0-based)
     * @param msg                error message
     */
    public AntlrError(Token tokenError, String location, int line, int charPositionInLine, String msg) {
        this(tokenError, location, line, charPositionInLine, msg, ErrorTypes.OTHER);
    }

    /**
     * Creates an ANTLR error with specified error type
     *
     * @param tokenError         the token where error occurred (may be null)
     * @param location           file path where error occurred
     * @param line               line number where error occurred (1-based)
     * @param charPositionInLine character position in line (0-based)
     * @param msg                error message
     * @param errorType          type of the error
     */
    public AntlrError(Token tokenError, String location, int line, int charPositionInLine, String msg, ErrorTypes errorType) {
        this(location, line, charPositionInLine, msg,
                (tokenError == null ? -1 : ((CodeUnitToken) tokenError).getCodeUnitStart()),
                (tokenError == null ? -1 : ((CodeUnitToken) tokenError).getCodeUnitStop()),
                (tokenError == null ? null : tokenError.getText()),
                errorType);
    }

    private AntlrError(String location, int line, int charPositionInLine, String msg,
            int start, int stop, String text, ErrorTypes errorType) {
        super(location, start, line, charPositionInLine);
        this.msg = msg;
        this.stop = stop;
        this.text = text;
        this.errorType = errorType;
    }

    /**
     * Creates an ANTLR error without token information
     *
     * @param location           file path where error occurred
     * @param line               line number where error occurred (1-based)
     * @param charPositionInLine character position in line (0-based)
     * @param msg                error message
     * @param errorType          type of the error
     */
    public AntlrError(String location, int line, int charPositionInLine, String msg, ErrorTypes errorType) {
        this(location, line, charPositionInLine, msg, -1, -1, null, errorType);
    }

    /**
     * Creates a copy of this error with adjusted positions
     *
     * @param offset       character offset to add
     * @param lineOffset   line number offset to add
     * @param inLineOffset in-line character position offset to add
     * @return new error instance with adjusted positions
     */
    public AntlrError copyWithOffset(int offset, int lineOffset, int inLineOffset) {
        return new AntlrError(getFilePath(), getLineNumber() + lineOffset,
                (getLineNumber() == 1 ? getCharPositionInLine() + inLineOffset : getCharPositionInLine()),
                msg,
                (getStart() == -1 ? -1 : getStart() + offset),
                (stop == -1 ? -1: stop + offset),
                text, errorType);
    }


    public String getMsg() {
        return msg;
    }

    public String getText() {
        return text;
    }

    public int getStart() {
        return getOffset();
    }

    public int getStop() {
        return stop;
    }

    public ErrorTypes getErrorType() {
        return errorType;
    }

    /**
     * Returns formatted string representation of the error.
     * Converts 0-based char position to 1-based for display.
     *
     * @return formatted error string with location info
     */
    @Override
    public String toString() {
        // ANTLR position in line is 0-based, GUI's is 1-based
        return getFilePath() + " line " + getLineNumber() + ':' + (getCharPositionInLine() + 1) + ' ' + getMsg();
    }
}