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
package org.pgcodekeeper.core.database.pg.schema;

/**
 * Interface for PostgreSQL objects that can contain index parameters.
 * Provides methods for adding index options, include columns, and tablespace settings.
 */
public interface PgIndexParamContainer {
    /**
     * Adds an index parameter (WITH clause option).
     *
     * @param key   parameter name
     * @param value parameter value
     */
    void addParam(String key, String value);

    /**
     * Adds a column to the INCLUDE clause of the index.
     *
     * @param include column name to include
     */
    void addInclude(String include);

    /**
     * Sets the tablespace for this index.
     *
     * @param tablespace tablespace name
     */
    void setTablespace(String tablespace);
}