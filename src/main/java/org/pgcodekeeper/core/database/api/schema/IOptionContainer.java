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
package org.pgcodekeeper.core.database.api.schema;

import java.util.*;

import org.pgcodekeeper.core.script.SQLScript;

/**
 * Interface for database objects that support storage and configuration options.
 * Provides functionality for managing key-value option pairs.
 */
public interface IOptionContainer extends IStatement {

    /**
     * Adds an option to this container.
     *
     * @param key the option key
     * @param value the option value
     */
    void addOption(String key, String value);

    /**
     * Gets all options for this container.
     *
     * @return a map of option keys to values
     */
    Map<String, String> getOptions();

    /**
     * Compares options between this container and a new container, generating SQL to update differences.
     *
     * @param newContainer the new container to compare against
     * @param script the script to append changes to
     */
    void compareOptions(IOptionContainer newContainer, SQLScript script);
}
