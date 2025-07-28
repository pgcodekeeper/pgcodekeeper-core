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
package org.pgcodekeeper.core.schema;

import org.pgcodekeeper.core.model.difftree.DbObjType;

/**
 * Base interface for all database statements and objects.
 * Provides common functionality for identifying and accessing database objects.
 */
public interface IStatement {
    /**
     * Gets the name of this statement.
     *
     * @return the statement name
     */
    String getName();

    /**
     * Gets the type of this database object.
     *
     * @return the database object type
     */
    DbObjType getStatementType();

    /**
     * Gets the database that contains this statement.
     *
     * @return the containing database
     */
    AbstractDatabase getDatabase();

    /**
     * Gets the parent statement that contains this statement.
     *
     * @return the parent statement, or null if this is a top-level object
     */
    IStatement getParent();

    /**
     * Gets the fully qualified name of this statement.
     *
     * @return the qualified name
     */
    String getQualifiedName();

    /**
     * Gets the comment associated with this statement.
     *
     * @return the comment, or null if no comment is set
     */
    String getComment();

    /**
     * Gets the bare name without qualifiers or arguments.
     *
     * @return the bare name
     */
    String getBareName();
}