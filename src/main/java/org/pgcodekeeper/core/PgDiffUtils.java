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
 **
 * Copyright 2006 StartNet s.r.o.
 *
 * Distributed under MIT license
 *******************************************************************************/
package org.pgcodekeeper.core;

import org.pgcodekeeper.core.sql.Keyword;
import org.pgcodekeeper.core.sql.Keyword.KeywordCategory;

import java.util.*;

/**
 * Utility class for handling quoting and unquoting of identifiers and literals in PostgreSQL.
 *
 * @author fordfrog
 */
public final class PgDiffUtils {

    /**
     * Checks if string is a valid PostgreSQL identifier.
     *
     * @param id            the identifier to check
     * @param allowKeywords whether to allow reserved keywords
     * @param allowCaps     whether to allow uppercase letters
     * @return true if valid identifier, false otherwise
     */
    public static boolean isValidId(String id, boolean allowKeywords, boolean allowCaps) {
        if (id.isEmpty()) {
            return true;
        }

        String lowerId = id.toLowerCase(Locale.ROOT);
        if (lowerId.startsWith("u&\"")) {
            return true;
        }

        for (int i = 0; i < id.length(); i++) {
            if (!isValidIdChar(id.charAt(i), allowCaps, i != 0)) {
                return false;
            }
        }

        if (!allowKeywords) {
            Keyword keyword = Keyword.KEYWORDS.get(allowCaps ? lowerId : id);
            return keyword == null || keyword.getCategory() == KeywordCategory.UNRESERVED_KEYWORD;
        }

        return true;
    }

    /**
     * Checks if character is valid for PostgreSQL identifiers.
     *
     * @param c the character to check
     * @return true if valid identifier character, false otherwise
     */
    public static boolean isValidIdChar(char c) {
        return isValidIdChar(c, true, true);
    }

    /**
     * Checks if character is valid for PostgreSQL identifiers.
     *
     * @param c           the character to check
     * @param allowCaps   whether to allow uppercase letters
     * @param allowDigits whether to allow digits (not in first position)
     * @return true if valid identifier character, false otherwise
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
     * If name contains only lower case characters and digits and is not
     * keyword, it is returned not quoted, otherwise the string is returned
     * quoted.
     *
     * @param name name
     * @return quoted string if needed, otherwise not quoted string
     */
    public static String getQuotedName(final String name) {
        return isValidId(name, false, false) ? name : quoteName(name);
    }

    /**
     * Quotes PostgreSQL identifier with double quotes.
     *
     * @param name the name to quote
     * @return quoted identifier
     */
    public static String quoteName(String name) {
        return '"' + name.replace("\"", "\"\"") + '"';
    }

    /**
     * Quotes string with single quotes for SQL.
     *
     * @param s the string to quote
     * @return quoted string
     */
    public static String quoteString(String s) {
        return '\'' + s.replace("'", "''") + '\'';
    }

    /**
     * Quotes string using dollar-quoting ($$) syntax.
     * <p>
     * Function equivalent to appendStringLiteralDQ in pgdump's dumputils.c
     *
     * @param contents the string to quote
     * @return dollar-quoted string
     */
    public static String quoteStringDollar(String contents) {
        final String suffixes = "_XXXXXXX";
        StringBuilder quote = new StringBuilder("$");
        int counter = 0;
        while (contents.contains(quote)) {
            quote.append(suffixes.charAt(counter++));
            counter %= suffixes.length();
        }

        quote.append('$');
        String dollar = quote.toString();
        return dollar + contents + dollar;
    }

    /**
     * Unquotes a double-quoted PostgreSQL identifier.
     *
     * @param name the quoted name
     * @return unquoted identifier
     */
    public static String unquoteQuotedName(String name) {
        return name.substring(1, name.length() - 1).replace("\"\"", "\"");
    }

    /**
     * Unquotes a single-quoted SQL string starting from specified position.
     *
     * @param s     the quoted string to unquote
     * @param start the starting position of the quoted content
     * @return unquoted string
     */
    public static String unquoteQuotedString(String s, int start) {
        return s.substring(start, s.length() - 1).replace("''", "'");
    }

    /**
     * Checks if text starts with identifier at specified offset.
     *
     * @param text   the text to check
     * @param id     the identifier to look for
     * @param offset the position to start checking
     * @return true if text starts with identifier at offset, false otherwise
     */
    public static boolean startsWithId(String text, String id, int offset) {
        if (offset != 0 && isValidIdChar(text.charAt(offset - 1))) {
            return false;
        }
        int rightChar = offset + id.length();
        if (rightChar < text.length() && isValidIdChar(text.charAt(rightChar))) {
            return false;
        }

        return text.startsWith(id, offset);
    }

    /**
     * Checks if language is valid for PostgreSQL (PLPGSQL or SQL).
     *
     * @param language the language to check
     * @return true if language is valid, false otherwise
     */
    public static boolean isValidLanguage(String language) {
        return "PLPGSQL".equalsIgnoreCase(language) || "SQL".equalsIgnoreCase(language);
    }

    /**
     * Wraps SQL in DO block with error handling.
     *
     * @param sbResult        the StringBuilder to append to
     * @param sbSQL           the SQL to wrap
     * @param expectedErrCode the expected error code to handle
     */
    public static void appendSqlWrappedInDo(StringBuilder sbResult, StringBuilder sbSQL, String expectedErrCode) {
        String body = sbSQL.toString().replace("\n", "\n\t");

        sbResult
                .append("DO $$")
                .append("\nBEGIN")
                .append("\n\t").append(body)
                .append("\nEXCEPTION WHEN OTHERS THEN")
                .append("\n\tIF (SQLSTATE = ").append(expectedErrCode).append(") THEN")
                .append("\n\t\tRAISE NOTICE '%, skip', SQLERRM;")
                .append("\n\tELSE")
                .append("\n\t\tRAISE;")
                .append("\n\tEND IF;")
                .append("\nEND; $$")
                .append("\nLANGUAGE 'plpgsql'");
    }

    private PgDiffUtils() {
    }

}
