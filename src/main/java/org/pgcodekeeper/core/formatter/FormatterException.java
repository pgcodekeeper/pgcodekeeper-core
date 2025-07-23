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
package org.pgcodekeeper.core.formatter;

/**
 * Exception thrown when errors occur during SQL formatting operations.
 * Wraps various formatting-related errors with appropriate context.
 */
public class FormatterException extends Exception {

    private static final long serialVersionUID = -3464810941906751101L;

    /**
     * Constructs a new FormatterException with no detail message.
     */
    public FormatterException() {
    }

    /**
     * Constructs a new FormatterException with the specified detail message.
     *
     * @param message the detail message describing the formatting error
     */
    public FormatterException(String message) {
        super(message);
    }

    /**
     * Constructs a new FormatterException with the specified cause.
     *
     * @param cause the underlying cause of the formatting error
     */
    public FormatterException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new FormatterException with the specified detail message and cause.
     *
     * @param message the detail message describing the formatting error
     * @param cause   the underlying cause of the formatting error
     */
    public FormatterException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new FormatterException with full configuration options.
     *
     * @param message            the detail message describing the formatting error
     * @param cause              the underlying cause of the formatting error
     * @param enableSuppression  whether exception suppression is enabled
     * @param writableStackTrace whether the stack trace should be writable
     */
    public FormatterException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
