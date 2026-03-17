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
package org.pgcodekeeper.core.database.api.loader;

import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.settings.ISettings;

import java.io.IOException;
import java.util.List;

/**
 * Interface for database loader
 */
public interface ILoader {

    /**
     * Loads the database schema.
     *
     * @return loaded database
     */
    IDatabase load() throws IOException, InterruptedException;

    /**
     * Loads the database schema and runs full expression analysis.
     *
     * @return loaded and fully analyzed database
     */
    IDatabase loadAndAnalyze() throws IOException, InterruptedException;

    /**
     * @return previously loaded database, or null if {@link #load()} has not been called
     */
    IDatabase getDatabase();

    /**
     * @return name identifying the database source (database name, file name, or project directory name)
     */
    String getDatabaseName();

    /**
     * @return configuration settings
     */
    ISettings getSettings();

    /**
     * @return unmodifiable list of errors during loading
     */
    List<Object> getErrors();
}
