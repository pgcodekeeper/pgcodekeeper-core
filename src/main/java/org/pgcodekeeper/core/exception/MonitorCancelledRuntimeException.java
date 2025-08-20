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

/**
 * Exception indicating that a monitoring operation was cancelled.
 * Thrown when an ongoing monitoring process is interrupted by user request or system shutdown.
 */
public final class MonitorCancelledRuntimeException extends RuntimeException {

    private static final long serialVersionUID = 1305941709453867664L;

    /**
     * Constructs exception with null message and cause.
     */
    public MonitorCancelledRuntimeException() {
        super();
    }

    /**
     * Constructs exception with specified message and cause.
     *
     * @param message the detail message
     * @param cause   the root cause exception
     */
    public MonitorCancelledRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs exception with specified message.
     *
     * @param message the detail message
     */
    public MonitorCancelledRuntimeException(String message) {
        super(message);
    }

    /**
     * Constructs exception with specified cause.
     *
     * @param cause the root cause exception
     */
    public MonitorCancelledRuntimeException(Throwable cause) {
        super(cause);
    }
}