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

/**
 * Enumeration of database object types.
 * The order defined here is used when building the list of objects for deployment.
 * Covers all major database object types.
 */
public enum DbObjType {
    DATABASE("DATABASE"),
    CAST("CAST"),
    USER("USER"),
    ROLE("ROLE"),
    ASSEMBLY("ASSEMBLY"),
    SCHEMA("SCHEMA"),
    EXTENSION("EXTENSION"),
    EVENT_TRIGGER("EVENT TRIGGER"),
    FOREIGN_DATA_WRAPPER("FOREIGN DATA WRAPPER"),
    SERVER("SERVER"),
    USER_MAPPING("USER MAPPING"),
    COLLATION("COLLATION"),
    TYPE("TYPE"),
    DOMAIN("DOMAIN"),
    SEQUENCE("SEQUENCE"),
    OPERATOR("OPERATOR"),
    FTS_PARSER("TEXT SEARCH PARSER"),
    FTS_TEMPLATE("TEXT SEARCH TEMPLATE"),
    FTS_DICTIONARY("TEXT SEARCH DICTIONARY"),
    FTS_CONFIGURATION("TEXT SEARCH CONFIGURATION"),
    TABLE("TABLE"),
    DICTIONARY("DICTIONARY"),
    COLUMN("COLUMN"),
    FUNCTION("FUNCTION"),
    PROCEDURE("PROCEDURE"),
    AGGREGATE("AGGREGATE"),
    INDEX("INDEX"),
    CONSTRAINT("CONSTRAINT"),
    VIEW("VIEW"),
    STATISTICS("STATISTICS"),
    TRIGGER("TRIGGER"),
    RULE("RULE"),
    POLICY("POLICY");

    private final String typeName;

    DbObjType(String typeName) {
        this.typeName = typeName;
    }

    /**
     * Gets the display name for this database object type.
     * 
     * @return the type name as used in SQL statements
     */
    public String getTypeName() {
        return typeName;
    }

    /**
     * Checks if this database object type is one of the specified types.
     *
     * @param types the types to check against
     * @return true if this type matches any of the specified types, false otherwise
     */
    public boolean in(DbObjType... types) {
        for (DbObjType type : types) {
            if (this == type) {
                return true;
            }
        }
        return false;
    }
}