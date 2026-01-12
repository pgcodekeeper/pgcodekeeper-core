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
package org.pgcodekeeper.core.model.exporter;

import java.io.IOException;
import java.io.Serial;

/**
 * Exception thrown when encountering directory-related errors during export operations.
 * Extends IOException to provide specific handling for directory manipulation failures.
 *
 * @author Alexander Levsha
 */
public class DirectoryException extends IOException {

    @Serial
    private static final long serialVersionUID = 2339456504336693884L;

    /**
     * Creates a new DirectoryException with no detail message.
     */
    public DirectoryException() {
    }

    /**
     * Creates a new DirectoryException with the specified detail message.
     * 
     * @param message the detail message
     */
    public DirectoryException(String message) {
        super(message);
    }

    /**
     * Creates a new DirectoryException with the specified cause.
     * 
     * @param cause the cause of this exception
     */
    public DirectoryException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates a new DirectoryException with the specified detail message and cause.
     * 
     * @param message the detail message
     * @param cause the cause of this exception
     */
    public DirectoryException(String message, Throwable cause) {
        super(message, cause);
    }
}
