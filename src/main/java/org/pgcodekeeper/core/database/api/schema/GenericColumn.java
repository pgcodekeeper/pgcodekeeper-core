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

import org.pgcodekeeper.core.PgDiffUtils;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * Represents a generic database object reference with schema, table, column, and type information.
 * Used for identifying and referencing database objects across different contexts.
 */
public class GenericColumn implements Serializable {

    @Serial
    private static final long serialVersionUID = -3196057456408062736L;

    // SONAR-OFF
    public final String schema;
    public final String table;
    public final String column;
    public final DbObjType type;
    // SONAR-ON

    /**
     * Creates a generic column with full specification.
     *
     * @param schema the schema name
     * @param table  the table name
     * @param column the column name
     * @param type   the database object type
     */
    public GenericColumn(String schema, String table, String column, DbObjType type) {
        this.schema = schema;
        this.table = table;
        this.column = column;
        this.type = type;
    }

    /**
     * Creates a generic column for a database object within a schema.
     *
     * @param schema the schema name
     * @param object the object name (table, view, function, etc.)
     * @param type   the database object type
     */
    public GenericColumn(String schema, String object, DbObjType type) {
        this(schema, object, null, type);
    }

    /**
     * Creates a generic column for a schema-level object.
     *
     * @param schema the schema name
     * @param type   the database object type
     */
    public GenericColumn(String schema, DbObjType type) {
        this(schema, null, type);
    }

    /**
     * Gets the name of the most specific object component.
     *
     * @return the column name if present, otherwise table name, otherwise schema name
     */
    public String getObjName() {
        if (column != null) {
            return column;
        }
        if (table != null) {
            return table;
        }
        if (schema != null) {
            return schema;
        }

        return "";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((column == null) ? 0 : column.hashCode());
        result = prime * result + ((schema == null) ? 0 : schema.hashCode());
        result = prime * result + ((table == null) ? 0 : table.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof GenericColumn col) {
            return Objects.equals(schema, col.schema)
                    && Objects.equals(table, col.table)
                    && Objects.equals(column, col.column)
                    && type == col.type;
        }

        return false;
    }

    /**
     * Gets the fully qualified name of this object.
     *
     * @return the qualified name as a string
     */
    public String getQualifiedName() {
        return appendQualifiedName(new StringBuilder()).toString();
    }

    protected StringBuilder appendQualifiedName(StringBuilder sb) {
        if (type == DbObjType.CAST) {
            sb.append(schema);
            return sb;
        }

        if (schema != null) {
            sb.append(PgDiffUtils.getQuotedName(schema));
        }
        if (table != null) {
            if (!sb.isEmpty()) {
                sb.append('.');
            }
            switch (type) {
                case FUNCTION:
                case PROCEDURE:
                case AGGREGATE:
                    sb.append(table);
                    break;
                default:
                    sb.append(PgDiffUtils.getQuotedName(table));
            }
        }
        if (column != null) {
            if (!sb.isEmpty()) {
                sb.append('.');
            }
            sb.append(PgDiffUtils.getQuotedName(column));
        }
        return sb;
    }

    @Override
    public String toString() {
        return appendQualifiedName(new StringBuilder())
                .append(" (").append(type).append(')')
                .toString();
    }
}