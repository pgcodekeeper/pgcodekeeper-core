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
package org.pgcodekeeper.core.database.api.schema;

import java.util.Collection;

/**
 * Interface representing a database containing schemas.
 * Provides access to database schemas and extends statement container functionality.
 */
public interface IDatabase extends IStatementContainer {
    /**
     * Gets all schemas in this database.
     *
     * @return a collection of schemas
     */
    Collection<? extends ISchema> getSchemas();

    /**
     * Gets a schema by name.
     *
     * @param name the schema name
     * @return the schema with the given name, or null if not found
     */
    ISchema getSchema(String name);

    /**
     * @param genericColumn - object reference
     * @return object from database by generic column
     */
    IStatement getStatement(GenericColumn genericColumn);
}
