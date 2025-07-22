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
package org.pgcodekeeper.core;

/**
 * Exception class for pgCodeKeeper specific exceptions.
 * <p>
 * This exception should be used for all application-specific error conditions.
 */
public class PgCodekeeperException extends Exception {

    private static final long serialVersionUID = 8958777207045999192L;

    /**
     * Constructs a new pgCodeKeeper exception with no detail message.
     */
    public PgCodekeeperException() {
        super();
    }

    /**
     * Constructs a new pgCodeKeeper exception with the specified detail message.
     *
     * @param message the detail message
     */
    public PgCodekeeperException(String message) {
        super(message);
    }

    /**
     * Constructs a new pgCodeKeeper exception with the specified cause.
     *
     * @param cause the cause
     */
    public PgCodekeeperException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new pgCodeKeeper exception with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause   the cause
     */
    public PgCodekeeperException(String message, Throwable cause) {
        super(message, cause);
    }
}
