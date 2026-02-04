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

import org.pgcodekeeper.core.database.api.schema.IStatement;

import java.io.Serial;

/**
 * Exception thrown when attempting to create a database object that already exists.
 * Used for cases when object creation fails due to naming conflicts.
 */
public class ObjectCreationException extends RuntimeException {

    private static final String WITHOUT_PARENT = "%s %s already exists"; //$NON-NLS-1$
    private static final String WITH_PARENT = "%s %s already exists for %s %s"; //$NON-NLS-1$

    @Serial
    private static final long serialVersionUID = -8514537124804597343L;

    /**
     * Constructs exception with null message and cause.
     */
    public ObjectCreationException() {
        super();
    }

    /**
     * Constructs exception with specified message.
     *
     * @param message the detail message
     */
    public ObjectCreationException(String message) {
        super(message);
    }

    /**
     * Constructs exception with specified cause.
     *
     * @param cause the root cause exception
     */
    public ObjectCreationException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs exception with specified message and cause.
     *
     * @param message the detail message
     * @param cause   the root cause exception
     */
    public ObjectCreationException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs exception with full exception details.
     *
     * @param message            the detail message
     * @param cause              the root cause exception
     * @param enableSuppression  whether suppression is enabled
     * @param writableStackTrace whether stack trace is writable
     */
    public ObjectCreationException(String message, Throwable cause,
                                   boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    /**
     * Constructs exception for existing database statement.
     *
     * @param st the statement that already exists
     */
    public ObjectCreationException(IStatement st) {
        super(WITHOUT_PARENT.formatted(st.getStatementType(), st.getName()));
    }

    /**
     * Constructs exception for existing database statement with parent context.
     *
     * @param st     the statement that already exists
     * @param parent the parent statement context
     */
    public ObjectCreationException(IStatement st, IStatement parent) {
        super(WITH_PARENT.formatted(st.getStatementType(), st.getName(),
                parent.getStatementType(), parent.getName()));
    }
}
