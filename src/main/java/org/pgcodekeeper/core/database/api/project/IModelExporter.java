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
package org.pgcodekeeper.core.database.api.project;

import org.pgcodekeeper.core.exception.PgCodeKeeperException;

import java.io.IOException;

public interface IModelExporter {

    /**
     * Exports selected objects as a new project structure.
     * Creates clean directory structure with only specified objects.
     *
     * @throws IOException if export operation fails
     */
    void exportProject() throws IOException;

    /**
     * Exports the complete database schema to directory structure.
     * Creates output directory and exports all database objects as SQL files.
     *
     * @throws IOException if export operation fails
     */
    void exportFull() throws IOException;

    /**
     * Exports only changed objects based on comparison between old and new schemas.
     * Handles object additions, deletions, and modifications.
     *
     * @throws IOException           if export operation fails
     * @throws PgCodeKeeperException if old database is null or directory issues occur
     */
    void exportPartial() throws IOException, PgCodeKeeperException;
}
