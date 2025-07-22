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

import java.util.List;

/**
 * Interface for reporting progress, warnings, errors, and query results during database operations.
 * <p>
 * Implementations of this interface are used to provide feedback during potentially long-running
 * database operations. The interface extends {@link AutoCloseable} to ensure proper resource cleanup.
 */
public interface IProgressReporter extends AutoCloseable {

    /**
     * Writes the current database name being processed to the progress report.
     */
    void writeDbName();

    /**
     * Writes an informational message to the progress report.
     *
     * @param message the informational message to display
     */
    void writeMessage(String message);

    /**
     * Writes a warning message to the progress report.
     *
     * @param message the warning message to display
     */
    void writeWarning(String message);

    /**
     * Writes an error message to the progress report.
     *
     * @param message the error message to display
     */
    void writeError(String message);

    /**
     * Terminates the progress reporting session.
     */
    void terminate();

    /**
     * Displays query results in the progress report.
     *
     * @param query  the SQL query that was executed
     * @param object the result set data to display (as rows of objects)
     */
    void showData(String query, List<List<Object>> object);

    /**
     * Reports the location of an error within a SQL statement.
     *
     * @param start  the starting position of the error in the SQL text
     * @param length the length of the erroneous section
     */
    void reportErrorLocation(int start, int length);

    /**
     * Closes the progress reporter by calling {@link #terminate()}.
     * <p>
     * This default implementation ensures the reporter is properly terminated
     * when used in try-with-resources blocks.
     */
    @Override
    default void close() {
        terminate();
    }
}
