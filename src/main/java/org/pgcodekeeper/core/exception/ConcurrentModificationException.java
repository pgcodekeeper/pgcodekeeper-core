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

import java.io.Serial;

/**
 * Exception thrown when concurrent modification of a resource is detected.
 * Indicates that a modification conflict occurred while multiple threads or processes
 * were attempting to modify the same resource.
 */
public final class ConcurrentModificationException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = -6952773835185629552L;

    /**
     * Constructs a new exception.
     */
    public ConcurrentModificationException() {
        super();
    }

    /**
     * Constructs a new exception with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause   the cause of the exception
     */
    public ConcurrentModificationException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new exception with the specified detail message.
     *
     * @param message the detail message
     */
    public ConcurrentModificationException(String message) {
        super(message);
    }

    /**
     * Constructs a new exception with the specified cause.
     *
     * @param cause the cause of the exception
     */
    public ConcurrentModificationException(Throwable cause) {
        super(cause);
    }
}