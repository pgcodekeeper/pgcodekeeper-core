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
package org.pgcodekeeper.core.database.base.schema;

import java.util.Objects;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.hasher.Hasher;

/**
 * Abstract base class for database column definitions.
 * Provides common functionality for columns across different database types.
 */
public abstract class AbstractColumn extends AbstractStatement implements ISubElement {

    protected static final String ALTER_COLUMN = "\n\tALTER COLUMN ";
    protected static final String COLLATE = " COLLATE ";
    protected static final String NULL = " NULL";
    protected static final String NOT_NULL = " NOT NULL";

    protected String type;
    protected String collation;
    protected boolean notNull;
    protected String defaultValue;

    @Override
    public DbObjType getStatementType() {
        return DbObjType.COLUMN;
    }

    protected AbstractColumn(String name) {
        super(name);
    }

    public void setDefaultValue(final String defaultValue) {
        this.defaultValue = defaultValue;
        resetHash();
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    /**
     * Returns the complete column definition including type, constraints, and other attributes.
     *
     * @return the full column definition as SQL string
     */
    public abstract String getFullDefinition();

    public void setNotNull(final boolean notNull) {
        this.notNull = notNull;
        resetHash();
    }

    public boolean isNotNull() {
        return notNull;
    }

    public void setType(final String type) {
        this.type = type;
        resetHash();
    }

    public String getType() {
        return type;
    }

    public void setCollation(final String collation) {
        this.collation = collation;
        resetHash();
    }

    public String getCollation() {
        return collation;
    }

    @Override
    public ObjectLocation getLocation() {
        ObjectLocation location = meta.getLocation();
        if (location == null) {
            location = parent.getLocation();
        }
        return location;
    }

    protected String getAlterTable(boolean only) {
        return ((AbstractTable) parent).getAlterTable(only);
    }

    @Override
    public boolean compare(IStatement obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof AbstractColumn col && super.compare(obj)) {
            return Objects.equals(type, col.type)
                    && Objects.equals(collation, col.collation)
                    && notNull == col.notNull
                    && Objects.equals(defaultValue, col.defaultValue);
        }

        return false;
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.put(type);
        hasher.put(collation);
        hasher.put(notNull);
        hasher.put(defaultValue);
    }

    @Override
    public AbstractColumn shallowCopy() {
        AbstractColumn colDst = getColumnCopy();
        copyBaseFields(colDst);
        colDst.setType(type);
        colDst.setCollation(collation);
        colDst.setNotNull(notNull);
        colDst.setDefaultValue(defaultValue);
        return colDst;
    }

    protected abstract AbstractColumn getColumnCopy();
}