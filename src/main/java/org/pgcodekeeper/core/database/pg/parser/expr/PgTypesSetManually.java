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
package org.pgcodekeeper.core.database.pg.parser.expr;

/**
 * Contains manually defined type constants used in SQL analyzing.
 */
public final class PgTypesSetManually {

    public static final String UNKNOWN = "unknown_unknown";
    public static final String EMPTY = "empty";

    public static final String COLUMN = "column";
    public static final String FUNCTION_COLUMN = "functionCol";
    public static final String FUNCTION_TABLE = "functionTable";

    public static final String QUALIFIED_ASTERISK = "qualifiedAsterisk";

    public static final String BIT = "bit";
    public static final String BOOLEAN = "boolean";
    public static final String INTEGER = "integer";
    public static final String SMALLINT = "smallint";
    public static final String BIGINT = "bigint";
    public static final String SERIAL = "serial";
    public static final String SMALLSERIAL = "smallserial";
    public static final String BIGSERIAL = "bigserial";
    public static final String NUMERIC = "numeric";
    public static final String DOUBLE = "double precision";
    public static final String BPCHAR = "bpchar";
    public static final String TEXT = "text";
    public static final String NAME = "name";
    public static final String XML = "xml";
    public static final String JSON = "json";
    public static final String JSONB = "jsonb";
    public static final String ANY = "any";
    public static final String ANYTYPE = "anyelement";
    public static final String ANYARRAY = "anyarray";
    public static final String ANYENUM = "anyenum";
    public static final String ANYRANGE = "anyrange";
    public static final String ANYNOARRAY = "anynonarray";

    public static final String DATE = "date";
    public static final String TIMETZ = "time with time zone";
    public static final String TIMESTAMPTZ = "timestamp with time zone";
    public static final String TIME = "time without time zone";
    public static final String TIMESTAMP = "timestamp without time zone";
    public static final String CURSOR = "refcursor";

    private PgTypesSetManually() {
        // only statics
    }
}
