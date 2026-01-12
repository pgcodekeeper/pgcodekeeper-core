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
package org.pgcodekeeper.core.exception;

import java.io.Serial;

/**
 * Exception thrown when XML reading operations fail during JDBC metadata processing.
 */
public class XmlReaderException extends Exception {

    @Serial
    private static final long serialVersionUID = 893099268209172548L;

    /**
     * Creates a new XML reader exception with no message or cause.
     */
    public XmlReaderException() {
    }

    /**
     * Creates a new XML reader exception with the specified message.
     *
     * @param message the detail message
     */
    public XmlReaderException(String message) {
        super(message);
    }

    /**
     * Creates a new XML reader exception with the specified cause.
     *
     * @param cause the cause of this exception
     */
    public XmlReaderException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates a new XML reader exception with the specified message and cause.
     *
     * @param message the detail message
     * @param cause   the cause of this exception
     */
    public XmlReaderException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new XML reader exception with full configuration.
     *
     * @param message            the detail message
     * @param cause              the cause of this exception
     * @param enableSuppression  whether suppression is enabled
     * @param writableStackTrace whether the stack trace should be writable
     */
    public XmlReaderException(String message, Throwable cause,
                              boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
