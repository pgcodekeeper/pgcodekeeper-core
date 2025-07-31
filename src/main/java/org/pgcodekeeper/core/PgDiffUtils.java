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
import org.pgcodekeeper.core.utils.IMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.*;
import java.util.stream.Stream;

/**
 * A utility class providing various helper methods.
 *
 * <p>This class contains methods for:
 * <ul>
 *   <li>PostgreSQL identifier validation and quoting</li>
 *   <li>String manipulation and quoting for SQL</li>
 *   <li>Hashing functions (MD5, SHA-256)</li>
 *   <li>Collection comparison utilities</li>
 *   <li>Error handling and string processing</li>
 *   <li>PostgreSQL-specific language validation</li>
 *   <li>SQL statement wrapping</li>
 * </ul>
 *
 * @author fordfrog
 */
public final class PgDiffUtils {

    private static final Logger LOG = LoggerFactory.getLogger(PgDiffUtils.class);
    private static final int ERROR_SUBSTRING_LENGTH = 20;
    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

    /**
     * Secure random number generator instance.
     */
    public static final Random RANDOM = new SecureRandom();

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
     * Computes hash of string using specified algorithm.
     *
     * @param s        the string to hash
     * @param instance the hash algorithm to use
     * @return the hash bytes
     * @throws NoSuchAlgorithmException if algorithm is not available
     */
    public static byte[] getHash(String s, String instance) throws NoSuchAlgorithmException {
        return MessageDigest.getInstance(instance)
                .digest(s.getBytes(StandardCharsets.UTF_8));
    }


    /**
     * Returns hexadecimal string representation of hash.
     *
     * @param s        the string to hash
     * @param instance the hash algorithm to use
     * @return hexadecimal hash string
     */
    public static String hash(String s, String instance) {
        try {
            byte[] hash = getHash(s, instance);
            StringBuilder sb = new StringBuilder(2 * hash.length);
            for (byte b : hash) {
                sb.append(HEX_CHARS[(b & 0xff) >> 4]);
                sb.append(HEX_CHARS[(b & 0x0f)]);
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            LOG.error(e.getLocalizedMessage(), e);
            return instance + "_ERROR_" + RANDOM.nextInt();
        }
    }

    /**
     * Returns MD5 hash of string as hexadecimal.
     *
     * @param s the string to hash
     * @return lowercase hex MD5 for UTF-8 representation of given string
     */
    public static String md5(String s) {
        return hash(s, "MD5");
    }

    /**
     * Returns SHA-256 hash of string as hexadecimal.
     *
     * @param s the string to hash
     * @return lowercase hex SHA-256 for UTF-8 representation of given string
     */
    public static String sha(String s) {
        return hash(s, "SHA-256");
    }

    /**
     * URL-encodes string using UTF-8 encoding.
     *
     * @param string the string to encode
     * @return URL-encoded string
     */
    public static String checkedEncodeUtf8(String string) {
        return URLEncoder.encode(string, StandardCharsets.UTF_8);
    }

    /**
     * Gets error substring from specified position with default length.
     *
     * @param s   the string to extract from
     * @param pos the starting position
     * @return substring of error text
     */
    public static String getErrorSubstr(String s, int pos) {
        return getErrorSubstr(s, pos, ERROR_SUBSTRING_LENGTH);
    }

    /**
     * Gets error substring from specified position with custom length.
     *
     * @param s   the string to extract from
     * @param pos the starting position
     * @param len the maximum length of substring
     * @return substring of error text
     */
    public static String getErrorSubstr(String s, int pos, int len) {
        if (pos >= s.length()) {
            return "";
        }
        return pos + len < s.length() ? s.substring(pos, pos + len) : s.substring(pos);
    }

    /**
     * Checks if progress monitor has been cancelled.
     *
     * @param monitor the progress monitor to check
     * @throws InterruptedException if monitor is cancelled
     */
    public static void checkCancelled(IMonitor monitor)
            throws InterruptedException {
        if (monitor != null && monitor.isCanceled()) {
            throw new InterruptedException();
        }
    }

    /**
     * Compares 2 collections for equality unorderedly as if they were {@link Set}s.<br>
     * Does not eliminate duplicate elements as sets do and counts them instead. Thus it achieves complementarity with
     * setlikeHashcode while not requiring it to eliminate duplicates, nor does it require a
     * <code>List.containsAll()</code> O(N^2) call here. In general, duplicate elimination is an undesired side-effect
     * of comparison using {@link Set}s, so this solution is overall better and only *slightly* slower.<br>
     * <br>
     * <p>
     * Performance: best case O(1), worst case O(N) + new {@link HashMap} (in case N1 == N2), assuming size() takes
     * constant time.
     *
     * @param c1 first collection
     * @param c2 second collection
     * @return true if collections contain same elements in any order, false otherwise
     */
    public static boolean setlikeEquals(Collection<?> c1, Collection<?> c2) {
        final int s1 = c1.size();
        if (s1 != c2.size()) {
            return false;
        }
        if (0 == s1) {
            // both are empty
            return true;
        }
        // mimic HashSet(Collection) constructor
        final float loadFactor = 0.75f;
        final Map<Object, Integer> map =
                new HashMap<>(Math.max((int) (s1 / loadFactor) + 1, 16), loadFactor);
        for (Object el1 : c1) {
            map.compute(el1, (k, i) -> i == null ? 1 : (i + 1));
        }
        for (Object el2 : c2) {
            Integer i = map.get(el2);
            if (i == null) {
                // c1.count(el2) < c2.count(el2)
                return false;
            }
            if (i == 1) {
                // the last or the only instance of el2 in c1
                map.remove(el2);
            } else {
                // counted one duplicate
                map.put(el2, i - 1);
            }
        }
        // if the map is not empty at the end it means that
        // not all duplicates in c1 were matched by those in c2
        return map.isEmpty();
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
     * Checks if string ends with suffix (case-insensitive).
     *
     * @param str    the string to check
     * @param suffix the suffix to look for
     * @return true if string ends with suffix (case-insensitive), false otherwise
     */
    public static boolean endsWithIgnoreCase(String str, String suffix) {
        int suffixLength = suffix.length();
        return str.regionMatches(true, str.length() - suffixLength, suffix, 0, suffixLength);
    }

    /**
     * Casts a Stream into Iterable. Stream is consumed after Iterable.iterator() is called.
     */
    public static <T> Iterable<T> sIter(Stream<T> stream) {
        return stream::iterator;
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
