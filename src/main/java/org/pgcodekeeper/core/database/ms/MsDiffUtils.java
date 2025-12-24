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
package org.pgcodekeeper.core.database.ms;

import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.sql.Keyword;
import org.pgcodekeeper.core.sql.Keyword.KeywordCategory;

/**
 * Utility class for handling quoting and unquoting of identifiers and literals in Microsoft SQL.
 */
public class MsDiffUtils {

    /**
     * Quotes an identifier using Microsoft SQL square bracket syntax.
     * Escapes any existing square brackets in the name.
     *
     * @param name the identifier to quote
     * @return the quoted identifier
     */
    public static String quoteName(String name) {
        return '[' + name.replace("]", "]]") + ']';
    }

    /**
     * Checks if a string is a valid Microsoft SQL identifier.
     *
     * @param id            the identifier to validate
     * @param allowKeywords whether to allow reserved keywords as identifiers
     * @param allowCaps     whether to allow uppercase letters
     * @return true if the identifier is valid, false otherwise
     */
    public static boolean isValidId(String id, boolean allowKeywords, boolean allowCaps) {
        if (id.isEmpty()) {
            return true;
        }

        for (int i = 0; i < id.length(); i++) {
            if (!isValidIdChar(id.charAt(i), allowCaps, i != 0)) {
                return false;
            }
        }

        if (!allowKeywords) {
            Keyword keyword = Keyword.KEYWORDS.get(id);
            return keyword == null || keyword.getCategory() == KeywordCategory.UNRESERVED_KEYWORD;
        }

        return true;
    }

    /**
     * Checks if character is valid for Microsoft SQL identifiers.
     *
     * @param c the character to check
     * @return true if character is valid for Microsoft SQL identifiers, false otherwise
     */
    public static boolean isValidIdChar(char c) {
        return isValidIdChar(c, true, true);
    }

    /**
     * Checks if character is valid for Microsoft SQL identifiers.
     *
     * @param c           the character to check
     * @param allowCaps   whether to allow uppercase letters
     * @param allowDigits whether to allow digits
     * @return true if character is valid for Microsoft SQL identifiers, false otherwise
     */
    public static boolean isValidIdChar(char c, boolean allowCaps, boolean allowDigits) {
        return (c >= 'a' && c <= 'z') ||
                (allowCaps && c >= 'A' && c <= 'Z') ||
                (allowDigits && c >= '0' && c <= '9') ||
                c == '_';
    }

    /**
     * If name contains only lower case characters and digits and is not
     * keyword, it is returned not quoted, otherwise the string is returned
     * quoted.
     *
     * @param name name
     *
     * @return quoted string if needed, otherwise not quoted string
     */
    public static String getQuotedName(final String name) {
        return isValidId(name, false, true) ? name : quoteName(name);
    }

    /**
     * Unquotes a square bracket-quoted identifier.
     *
     * @param name the quoted identifier
     * @return the unquoted identifier
     */
    public static String unquoteQuotedName(String name) {
        return name.substring(1, name.length() - 1).replace("]]", "]");
    }

    /**
     * Removes square brackets from an identifier if present.
     *
     * @param name the identifier to process
     * @return the identifier without surrounding brackets
     */
    public static String getUnQuotedName(String name) {
        return name.contains("[") ? name.substring(1).replace("]", "") : name;
    }

    /**
     * Checks if a schema is a Microsoft SQL Server system schema.
     *
     * @param schema the schema name to check
     * @return true if the schema is 'sys', false otherwise
     */
    public static boolean isSystemSchema(String schema) {
        return Consts.SYS.equalsIgnoreCase(schema);
    }

    private MsDiffUtils() {
    }
}
