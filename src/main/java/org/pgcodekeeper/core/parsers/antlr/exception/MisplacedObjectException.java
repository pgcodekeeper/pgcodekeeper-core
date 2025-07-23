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
package org.pgcodekeeper.core.parsers.antlr.exception;

import org.antlr.v4.runtime.Token;

/**
 * Exception indicating incorrect placement or reference to a database object.
 * Thrown when an object is referenced in a context where it doesn't belong.
 */
public class MisplacedObjectException extends UnresolvedReferenceException {

    private static final long serialVersionUID = -8377509522524043609L;

    /**
     * Constructs exception with the error token location.
     *
     * @param errorToken the token where error occurred
     */
    public MisplacedObjectException(Token errorToken) {
        super(errorToken);
    }

    /**
     * Constructs exception with cause and error token location.
     *
     * @param cause      the root cause exception
     * @param errorToken the token where error occurred
     */
    public MisplacedObjectException(Throwable cause, Token errorToken) {
        super(cause, errorToken);
    }

    /**
     * Constructs exception with full exception details.
     *
     * @param message            the detail message
     * @param cause              the root cause exception
     * @param enableSuppression  whether suppression is enabled
     * @param writableStackTrace whether stack trace is writable
     * @param errorToken         the token where error occurred
     */
    public MisplacedObjectException(String message, Throwable cause,
            boolean enableSuppression, boolean writableStackTrace, Token errorToken) {
        super(message, cause, enableSuppression, writableStackTrace, errorToken);
    }

    /**
     * Constructs exception with formatted message.
     *
     * @param format   the message format string
     * @param errToken the token where error occurred
     */
    public MisplacedObjectException(String format, Token errToken) {
        super(format, errToken);
    }
}
