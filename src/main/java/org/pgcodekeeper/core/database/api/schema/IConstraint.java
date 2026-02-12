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

import java.util.Collection;

/**
 * Interface for database table constraints.
 * Provides common functionality for all constraint types including primary keys,
 * foreign keys, unique constraints, and check constraints.
 */
public interface IConstraint extends ISubElement {
    /**
     * Checks if this constraint is a primary key constraint.
     *
     * @return true if this is a primary key constraint
     */
    default boolean isPrimaryKey() {
        return false;
    }

    /**
     * Gets the SQL definition of this constraint.
     *
     * @return the constraint definition
     */
    String getDefinition();

    /**
     * Gets the columns involved in this constraint.
     *
     * @return a collection of column names
     */
    Collection<String> getColumns();

    /**
     * Checks if this constraint involves the specified column.
     *
     * @param name the column name to check
     * @return true if the column is part of this constraint
     */
    boolean containsColumn(String name);

    /**
     * Gets the name of the table this constraint belongs to.
     *
     * @return the table name
     */
    String getTableName();

    @Override
    default DbObjType getStatementType() {
        return DbObjType.CONSTRAINT;
    }
}
