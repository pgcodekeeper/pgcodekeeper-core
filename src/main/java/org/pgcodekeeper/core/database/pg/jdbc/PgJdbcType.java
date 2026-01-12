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
package org.pgcodekeeper.core.database.pg.jdbc;

import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.database.pg.PgDiffUtils;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.api.schema.GenericColumn;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a PostgreSQL data type with schema qualification and array type handling.
 * Provides methods for generating qualified type names and managing type dependencies.
 */
public class PgJdbcType {

    public static final Map<String, String> DATA_TYPE_ALIASES;

    static {
        Map<String, String> aliases = new HashMap<>();

        // format_type.c, format_type_internal function
        aliases.put("bit", "bit");
        aliases.put("bool", "boolean");
        aliases.put("bpchar", "character");
        aliases.put("float4", "real");
        aliases.put("float8", "double precision");
        aliases.put("int2", "smallint");
        aliases.put("int4", "integer");
        aliases.put("int8", "bigint");
        aliases.put("numeric", "numeric");
        aliases.put("interval", "interval");
        aliases.put("time", "time without time zone");
        aliases.put("timetz", "time with time zone");
        aliases.put("timestamp", "timestamp without time zone");
        aliases.put("timestamptz", "timestamp with time zone");
        aliases.put("varbit", "bit varying");
        aliases.put("varchar", "character varying");

        DATA_TYPE_ALIASES = Collections.unmodifiableMap(aliases);
    }

    private final long oid;
    private final String typeName;
    private final String parentSchema;
    private final boolean isArrayType;
    private final long lastSysOid;

    /**
     * Creates a new JDBC type representation.
     * Array types have names beginning with underscore and have 0 in typarray column.
     * Vector types have non-zero typarray values and are not converted to simple arrays.
     *
     * @param oid the type OID
     * @param typeName the type name
     * @param typelem the element type OID for arrays
     * @param typarray the array type OID
     * @param parentSchema the schema containing this type
     * @param elemname the element type name for arrays
     * @param lastSysOid the last system OID for dependency checking
     */
    public PgJdbcType(long oid, String typeName, long typelem, long typarray, String parentSchema,
                      String elemname, long lastSysOid) {
        this.oid = oid;
        this.parentSchema = parentSchema;
        this.isArrayType = typarray == 0L && typelem != 0L;
        this.typeName = isArrayType ? elemname : typeName;
        this.lastSysOid = lastSysOid;
    }

    /**
     * Returns the schema-qualified type name, applying aliases for pg_catalog types.
     *
     * @param targetSchemaName the target schema name for qualification comparison
     * @return the qualified type name
     */
    public String getSchemaQualifiedName(String targetSchemaName) {
        if (Consts.PG_CATALOG.equals(parentSchema)) {
            String dealias = DATA_TYPE_ALIASES.get(typeName);
            return dealias == null ? PgDiffUtils.getQuotedName(typeName) : dealias;
        }

        String qname = PgDiffUtils.getQuotedName(typeName);
        if (!targetSchemaName.equals(parentSchema)) {
            qname = PgDiffUtils.getQuotedName(parentSchema) + '.' + qname;
        }
        return qname;
    }

    /**
     * Returns the schema-qualified type name with empty target schema.
     *
     * @return the qualified type name
     */
    public String getSchemaQualifiedName() {
        return getSchemaQualifiedName("");
    }

    /**
     * Returns the full type name with schema qualification and array notation.
     * If the type's schema differs from targetSchemaName, the returned name is schema-qualified.
     * Array types have "[]" appended to the end.
     *
     * @param targetSchemaName the target schema name for qualification comparison
     * @return the full type name with array notation if applicable
     */
    public String getFullName(String targetSchemaName) {
        String schemaQualName = getSchemaQualifiedName(targetSchemaName);
        return isArrayType ? schemaQualName + "[]" : schemaQualName;
    }

    /**
     * Returns the full type name with empty target schema.
     *
     * @return the full type name with array notation if applicable
     */
    public String getFullName() {
        return getFullName("");
    }

    /**
     * Returns the GenericColumn representation containing the type's schema and name.
     *
     * @return the GenericColumn for this type
     */
    public GenericColumn getQualifiedName() {
        if (Consts.PG_CATALOG.equals(parentSchema)) {
            String dealias = DATA_TYPE_ALIASES.get(typeName);
            return new GenericColumn(parentSchema, dealias == null ? typeName : dealias,
                    DbObjType.TYPE);
        }
        return new GenericColumn(parentSchema, typeName, DbObjType.TYPE);
    }

    /**
     * Adds this type as a dependency to the specified statement if it's a user type.
     *
     * @param st the statement to add the dependency to
     */
    public void addTypeDepcy(AbstractStatement st) {
        if (oid > lastSysOid && !PgDiffUtils.isSystemSchema(parentSchema)) {
            st.addDependency(new GenericColumn(parentSchema, typeName, DbObjType.TYPE));
        }
    }
}
