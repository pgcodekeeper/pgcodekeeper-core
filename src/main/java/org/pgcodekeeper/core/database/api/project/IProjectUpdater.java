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

import java.io.IOException;

public interface IProjectUpdater {

    /**
     * Performs partial update of database project.
     * Updates only changed objects with safe backup and restore on failure.
     *
     * @throws IOException if update operation fails
     */
    void updatePartial() throws IOException;

    /**
     * Performs full update of database project.
     * Completely regenerates project structure with optional overrides preservation.
     *
     * @param projectOnly whether to preserve overrides directory during update
     * @throws IOException if update operation fails
     */
    void updateFull(boolean projectOnly) throws IOException;
}
