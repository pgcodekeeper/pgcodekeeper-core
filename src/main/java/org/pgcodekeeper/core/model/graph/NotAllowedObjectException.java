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
package org.pgcodekeeper.core.model.graph;

/**
 * Exception indicating that a {@code DbObjType} object
 * is not allowed in the current context.
 */
public class NotAllowedObjectException extends RuntimeException {

    private static final long serialVersionUID = -283715845801619786L;

    /**
     * Constructs a new NotAllowedObjectException with no detail message.
     */
    public NotAllowedObjectException() {
        super();
    }

    /**
     * Constructs a new NotAllowedObjectException with the specified detail message.
     *
     * @param message the detail message describing the restriction
     */
    public NotAllowedObjectException(String message) {
        super(message);
    }

    /**
     * Constructs a new NotAllowedObjectException with the specified cause.
     *
     * @param cause the underlying cause of this exception
     */
    public NotAllowedObjectException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new NotAllowedObjectException with the specified detail message and cause.
     *
     * @param message the detail message describing the restriction
     * @param cause   the underlying cause of this exception
     */
    public NotAllowedObjectException(String message, Throwable cause) {
        super(message, cause);
    }
}