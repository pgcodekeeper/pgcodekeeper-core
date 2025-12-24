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
package org.pgcodekeeper.core.database.ch;

import org.pgcodekeeper.core.Consts;

/**
 * Utility class for handling quoting and unquoting of identifiers and literals in ClickHouse.
 */
public class ChDiffUtils {

    /**
     * Returns the name quoted with backticks (`) if it's not a valid identifier.
     * If {@code name} is already valid, it is returned unchanged.
     *
     * @param name the name to check and quote if needed
     * @return the original name (if valid) or a backtick-quoted version
     */
    public static String getQuotedName(String name) {
        return isValidId(name, true) ? name : quoteName(name);
    }

    /**
     * Wraps the name in backticks (`) if not already quoted.
     *
     * @param name the name to quote
     * @return the quoted name, or original if already quoted
     */
    public static String quoteName(String name) {
        return name.startsWith("`") ? name : '`' + name + '`';
    }

    /**
     * Quotes a string literal with single quotes (') if unquoted.
     *
     * @param name the string to quote (e.g., a SQL value)
     * @return the quoted string, or original if already quoted
     */
    public static String quoteLiteralName(String name) {
        return name.startsWith("'") ? name : '\'' + name + '\'';
    }

    /**
     * Checks if the given string is a valid ClickHouse identifier.
     *
     * @param id        the identifier to validate
     * @param allowCaps whether to allow uppercase letters in the identifier
     * @return true if the identifier is valid, false otherwise
     */
    public static boolean isValidId(String id, boolean allowCaps) {
        if (id.isEmpty()) {
            return true;
        }

        for (int i = 0; i < id.length(); i++) {
            if (!isValidIdChar(id.charAt(i), allowCaps, i != 0)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Checks if character is valid for ClickHouse identifiers.
     *
     * @param c           the character to check
     * @param allowCaps   whether to allow uppercase letters
     * @param allowDigits whether to allow digits
     * @return true if character is valid for ClickHouse identifiers, false otherwise
     */
    public static boolean isValidIdChar(char c, boolean allowCaps, boolean allowDigits) {
        if (c >= 'a' && c <= 'z') {
            return true;
        }

        if (allowCaps && c >= 'A' && c <= 'Z') {
            return true;
        }

        if (allowDigits && c >= '0' && c <= '9') {
            return true;
        }

        return c == '_';
    }

    /**
     * Unquotes a backtick-quoted identifier.
     * Handles escaped backticks (``) by converting them to single backticks.
     *
     * @param name the quoted identifier to unquote
     * @return the unquoted identifier
     */
    public static String unQuoteName(String name) {
        return name.startsWith("`") ? name.substring(1, name.length() - 1).replace("``", "`") : name;
    }

    /**
     * Unquotes a single-quoted string literal.
     * Handles escaped single quotes ('') by converting them to single quotes.
     *
     * @param name the quoted string to unquote
     * @return the unquoted string
     */
    public static String unQuoteLiteralName(String name) {
        return name.startsWith("'") ? name.substring(1, name.length() - 1).replace("''", "'") : name;
    }

    /**
     * Checks if a schema is a ClickHouse system schema.
     *
     * @param schema the schema name to check
     * @return true if the schema is 'system' or 'information_schema', false otherwise
     */
    public static boolean isSystemSchema(String schema) {
        return Consts.SYSTEM.equalsIgnoreCase(schema)
                || Consts.INFORMATION_SCHEMA.equalsIgnoreCase(schema);
    }

    private ChDiffUtils() {
    }
}