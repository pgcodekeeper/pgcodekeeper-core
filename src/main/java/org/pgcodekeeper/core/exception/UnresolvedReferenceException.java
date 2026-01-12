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
package org.pgcodekeeper.core.exception;

import org.antlr.v4.runtime.Token;

import java.io.Serial;

/**
 * Exception thrown when a database object reference cannot be resolved.
 * Indicates that a referenced object was not found in the database schema.
 */
public class UnresolvedReferenceException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 3362236974343429554L;

    private final transient Token errorToken;

    /**
     * Constructs exception with error token location.
     *
     * @param errorToken the token where unresolved reference occurred
     */
    public UnresolvedReferenceException(Token errorToken) {
        super();
        this.errorToken = errorToken;
    }

    /**
     * Constructs exception with message and error token location.
     *
     * @param message    the detail message
     * @param errorToken the token where unresolved reference occurred
     */
    public UnresolvedReferenceException(String message, Token errorToken) {
        super(message);
        this.errorToken = errorToken;
    }


    /**
     * Constructs exception with cause and error token location.
     *
     * @param cause      the root cause exception
     * @param errorToken the token where unresolved reference occurred
     */
    public UnresolvedReferenceException(Throwable cause, Token errorToken) {
        super(cause);
        this.errorToken = errorToken;
    }

    /**
     * Constructs exception with message, cause and error token location.
     *
     * @param message    the detail message
     * @param cause      the root cause exception
     * @param errorToken the token where unresolved reference occurred
     */
    public UnresolvedReferenceException(String message, Throwable cause, Token errorToken) {
        super(message, cause);
        this.errorToken = errorToken;
    }

    /**
     * Constructs exception with full exception details and error token location.
     *
     * @param message            the detail message
     * @param cause              the root cause exception
     * @param enableSuppression  whether suppression is enabled
     * @param writableStackTrace whether stack trace is writable
     * @param errorToken         the token where unresolved reference occurred
     */
    public UnresolvedReferenceException(String message, Throwable cause,
            boolean enableSuppression, boolean writableStackTrace, Token errorToken) {
        super(message, cause, enableSuppression, writableStackTrace);
        this.errorToken = errorToken;
    }

    /**
     * Gets the token where unresolved reference occurred.
     *
     * @return the error token
     */
    public Token getErrorToken() {
        return errorToken;
    }
}
