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

/**
 * Interface for database objects that are contained within a schema and can be found via search path.
 * Provides methods for accessing the containing schema and database.
 */
public interface ISearchPath extends IStatement {
    /**
     * Gets the schema that contains this object.
     *
     * @return the containing schema
     */
    default ISchema getContainingSchema() {
        return (ISchema) getParent();
    }

    /**
     * Gets the name of the schema that contains this object.
     *
     * @return the schema name
     */
    default String getSchemaName() {
        return getContainingSchema().getName();
    }

    /**
     * Gets the database that contains this object.
     *
     * @return the containing database
     */
    @Override
    default IDatabase getDatabase() {
        return (IDatabase) getContainingSchema().getParent();
    }

    @Override
    default ObjectReference toObjectReference() {
        return new ObjectReference(getSchemaName(), getName(), getStatementType());
    }
}
