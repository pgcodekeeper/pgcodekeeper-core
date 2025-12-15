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
package org.pgcodekeeper.core.database.base.schema.meta;

import org.pgcodekeeper.core.database.api.schema.IConstraint;
import org.pgcodekeeper.core.database.api.schema.ISchema;
import org.pgcodekeeper.core.database.api.schema.ObjectLocation;

import java.io.Serial;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Represents a database constraint metadata object.
 * Provides information about table constraints including primary keys and column references.
 */
public final class MetaConstraint extends MetaStatement implements IConstraint {

    @Serial
    private static final long serialVersionUID = 1801686478824411463L;

    private boolean isPrimaryKey;
    private final Set<String> columns = new HashSet<>();

    /**
     * Creates a new constraint metadata object.
     *
     * @param object the object location information
     */
    public MetaConstraint(ObjectLocation object) {
        super(object);
    }

    @Override
    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    @Override
    public Set<String> getColumns() {
        return Collections.unmodifiableSet(columns);
    }

    @Override
    public boolean containsColumn(String name) {
        return columns.contains(name);
    }

    public void setPrimaryKey(boolean isPrimaryKey) {
        this.isPrimaryKey = isPrimaryKey;
    }

    /**
     * Adds a column to this constraint.
     *
     * @param column the column name to add
     */
    public void addColumn(String column) {
        columns.add(column);
    }

    /**
     * Returns the containing schema of this constraint.
     * This operation is not supported for metadata constraints.
     *
     * @return never returns normally
     * @throws IllegalStateException always thrown as this operation is unsupported
     */
    @Override
    public ISchema getContainingSchema() {
        throw new IllegalStateException("Unsupported operation");
    }

    /**
     * Returns the schema name of this constraint.
     *
     * @return the schema name
     */
    @Override
    public String getSchemaName() {
        return getObject().getSchema();
    }

    /**
     * Returns the table name of this constraint.
     *
     * @return the table name
     */
    @Override
    public String getTableName() {
        return getObject().getTable();
    }

    /**
     * Returns the constraint definition.
     * This operation is not supported for metadata constraints.
     *
     * @return never returns normally
     * @throws IllegalStateException always thrown as this operation is unsupported
     */
    @Override
    public String getDefinition() {
        throw new IllegalStateException("Unsupported operation");
    }
}
