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
 **
 * Copyright 2006 StartNet s.r.o.
 *
 * Distributed under MIT license
 *******************************************************************************/
package org.pgcodekeeper.core.database.pg;

import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.sql.Keyword;
import org.pgcodekeeper.core.sql.Keyword.KeywordCategory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Utility class for handling quoting and unquoting of identifiers and literals in PostgreSQL.
 *
 * @author fordfrog
 */
public final class PgDiffUtils {

    private static final int MAX_IDENTIFIER_LEN = 64;

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

    private PgDiffUtils() {
    }

    /**
     * Create a name for an implicitly created index, sequence, constraint, extended statistics, etc.
     * <p>
     * The result is "tableName_columnName_label", but the table and column names
     * are truncated if needed to keep the total length under 63 bytes.
     * The suffix is never truncated.
     * <p>
     * When truncation is needed, the longer name (table or column) is shortened first.
     * This ensures the result stays within PostgreSQL's 63 bytes limit.
     * <p>
     * <b>Note:</b> This implementation assumes the database encoding is UTF-8.
     *
     * @param tableName  the table name (name1)
     * @param columnName the column name (name2), can be null
     * @param suffix     the suffix (like "pkey", "not_null"), can be null
     * @return the generated name with proper truncation
     * @see <a href="https://github.com/postgres/postgres/blob/master/src/backend/commands/indexcmds.c#L2517">PostgreSQL makeObjectName</a>
     */
    public static String getDefaultObjectName(String tableName, String columnName, String suffix) {
        byte[] tableBytes = tableName.getBytes(StandardCharsets.UTF_8);
        byte[] columnBytes = columnName != null ? columnName.getBytes(StandardCharsets.UTF_8) : new byte[0];
        byte[] suffixBytes = suffix != null ? suffix.getBytes(StandardCharsets.UTF_8) : new byte[0];

        int tableLen = tableBytes.length;
        int columnLen = columnBytes.length;

        int overhead = 0;
        if (columnName != null) {
            overhead++;
        }
        if (suffix != null) {
            overhead += suffixBytes.length + 1;
        }

        int remainingBytes = MAX_IDENTIFIER_LEN - 1 - overhead;

        while (tableLen + columnLen > remainingBytes) {
            if (tableLen > columnLen) {
                tableLen--;
            } else {
                columnLen--;
            }
        }

        tableLen = clipToValidUtf8(tableBytes, tableLen);
        if (columnName != null) {
            columnLen = clipToValidUtf8(columnBytes, columnLen);
        }

        try {
            var byteStream = new ByteArrayOutputStream(MAX_IDENTIFIER_LEN);
            byteStream.write(tableBytes, 0, tableLen);

            if (columnName != null) {
                byteStream.write('_');
                byteStream.write(columnBytes, 0, columnLen);
            }

            if (suffix != null) {
                byteStream.write('_');
                byteStream.write(suffixBytes);
            }

            return byteStream.toString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Error constructing object name", e);
        }
    }

    /**
     * Adjusts the clip length to avoid cutting a UTF-8 character in the middle.
     * <p>
     * If the requested length would split a multibyte UTF-8 character,
     * this returns the position where the last complete character starts.
     * This prevents creating invalid UTF-8 sequences.
     *
     * @param bytes  the UTF-8 encoded byte array
     * @param length the desired clip length
     * @return the adjusted length that doesn't split UTF-8 characters
     */
    private static int clipToValidUtf8(byte[] bytes, int length) {
        if (length <= 0) return 0;
        if (length >= bytes.length) return bytes.length;

        int startOfLastChar = length - 1;
        while (startOfLastChar >= 0 && (bytes[startOfLastChar] & 0xC0) == 0x80) {
            startOfLastChar--;
        }

        if (startOfLastChar < 0) return 0;

        int firstByte = bytes[startOfLastChar] & 0xFF;
        int charLen;

        if ((firstByte & 0xE0) == 0xC0) {
            charLen = 2;
        } else if ((firstByte & 0xF0) == 0xE0) {
            charLen = 3;
        } else if ((firstByte & 0xF8) == 0xF0) {
            charLen = 4;
        } else {
            charLen = 1;
        }

        if (startOfLastChar + charLen > length) {
            return startOfLastChar;
        }

        return length;
    }

    /**
     * Checks if a schema is a PostgreSQL system schema.
     *
     * @param schema the schema name to check
     * @return true if the schema is 'pg_catalog' or 'information_schema', false otherwise
     */
    public static boolean isSystemSchema(String schema) {
        return Consts.PG_CATALOG.equalsIgnoreCase(schema)
                || Consts.INFORMATION_SCHEMA.equalsIgnoreCase(schema);
    }
}
