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
package org.pgcodekeeper.core;

import org.pgcodekeeper.core.database.api.jdbc.IJdbcConnector;
import org.pgcodekeeper.core.database.api.schema.DatabaseType;
import org.pgcodekeeper.core.database.ch.jdbc.ChJdbcConnector;
import org.pgcodekeeper.core.database.ms.jdbc.MsJdbcConnector;
import org.pgcodekeeper.core.database.pg.jdbc.PgJdbcConnector;
import org.pgcodekeeper.core.localizations.Messages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.io.Serializable;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.*;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

/**
 * Utility class providing common helper methods for various operations including
 * serialization, XML parsing, schema validation, and string manipulation.
 */
public final class Utils {

    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);
    private static final int ERROR_SUBSTRING_LENGTH = 20;
    /**
     * Secure random number generator instance.
     */
    private static final Random RANDOM = new SecureRandom();
    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

    /**
     * Serializes an object to a file at the specified path.
     *
     * @param path   the string path to the output file
     * @param object the serializable object to write
     */
    public static void serialize(String path, Serializable object) {
        serialize(Paths.get(path), object);
    }

    /**
     * Serializes an object to a file at the specified path.
     *
     * @param path   - full path to file where the serialized object will be
     * @param object - the object that you want to serialize
     */
    public static void serialize(Path path, Serializable object) {
        try {
            if (Files.notExists(path)) {
                Files.createDirectories(path.getParent());
            }
            try (ObjectOutputStream oos = new ObjectOutputStream(Files.newOutputStream(path))) {
                oos.writeObject(object);
                oos.flush();
            }
        } catch (IOException e) {
            LOG.debug(Messages.Utils_log_err_serialize, e);
        }
    }

    /**
     * Parses XML content from a Reader with secure configuration.
     *
     * @param reader the Reader containing XML content
     * @return the parsed XML Document
     * @throws ParserConfigurationException if a DocumentBuilder cannot be created
     * @throws SAXException                 if XML parsing fails
     * @throws IOException                  if an I/O error occurs
     */
    public static Document readXml(Reader reader) throws ParserConfigurationException, SAXException, IOException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        // Disable DOCTYPE declarations entirely
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true); //$NON-NLS-1$

        // Disable external general entities
        factory.setFeature("http://xml.org/sax/features/external-general-entities", false); //$NON-NLS-1$

        // Disable external parameter entities
        factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false); //$NON-NLS-1$

        // Prohibit the use of all protocols by external entities (JAXP 1.5+)
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, ""); //$NON-NLS-1$
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, ""); //$NON-NLS-1$

        Document doc = factory.newDocumentBuilder().parse(new InputSource(reader));
        doc.normalize();
        return doc;
    }

    /**
     * Writes an XML document to the output stream
     *
     * @param xml - {@link Document} witch store xml document
     * @param encrypt - if true, enables secure XML processing
     * @param stream - The output stream to write the XML document to
     * @throws TransformerException if an error occurred during the XML transformation
     */
    public static void writeXml(Document xml, boolean encrypt, StreamResult stream) throws TransformerException {
        TransformerFactory factory = TransformerFactory.newInstance();
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, ""); //$NON-NLS-1$
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_STYLESHEET, ""); //$NON-NLS-1$

        if (encrypt) {
            factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        }

        Transformer tf = factory.newTransformer();
        tf.setOutputProperty(OutputKeys.INDENT, "yes"); //$NON-NLS-1$
        tf.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4"); //$NON-NLS-1$ //$NON-NLS-2$
        tf.transform(new DOMSource(xml), stream);
    }

    /**
     * Checks if a schema is a system schema for the given database type.
     *
     * @param schema the schema name to check
     * @param dbType the database type
     * @return true if the schema is a system schema, false otherwise
     */
    public static boolean isSystemSchema(String schema, DatabaseType dbType) {
        return switch (dbType) {
            case PG -> isPgSystemSchema(schema);
            case MS -> isMsSystemSchema(schema);
            case CH -> isChSystemSchema(schema);
        };
    }

    /**
     * Returns a properly quoted name for the given database type.
     *
     * @param name   the name to quote
     * @param dbType the database type
     * @return the quoted name
     */
    public static String getQuotedName(String name, DatabaseType dbType) {
        return getQuoter(dbType).apply(name);
    }

    /**
     * Returns a quoting function for the given database type.
     *
     * @param dbType the database type
     * @return a function that quotes names appropriately for the database
     */
    public static UnaryOperator<String> getQuoter(DatabaseType dbType) {
        return switch (dbType) {
            case PG -> PgDiffUtils::getQuotedName;
            case MS -> MsDiffUtils::quoteName;
            case CH -> ChDiffUtils::getQuotedName;
        };
    }

    /**
     * Returns JdbcConnector for the given database type. Temporary solution until the new API is finished.
     *
     * @param dbType the database type
     * @param url full jdbc connection string
     * @return JdbcConnector for the given database type.
     */
    public static IJdbcConnector getJdbcConnectorByType(DatabaseType dbType, String url) {
        return switch (dbType) {
            case PG -> new PgJdbcConnector(url);
            case MS -> new MsJdbcConnector(url);
            case CH -> new ChJdbcConnector(url);
        };
    }

    /**
     * Checks if a schema is a ClickHouse system schema.
     *
     * @param schema the schema name to check
     * @return true if the schema is 'system' or 'information_schema', false otherwise
     */
    public static boolean isChSystemSchema(String schema) {
        return Consts.SYSTEM.equalsIgnoreCase(schema)
                || Consts.INFORMATION_SCHEMA.equalsIgnoreCase(schema);
    }

    /**
     * Checks if a schema is a PostgreSQL system schema.
     *
     * @param schema the schema name to check
     * @return true if the schema is 'pg_catalog' or 'information_schema', false otherwise
     */
    public static boolean isPgSystemSchema(String schema) {
        return Consts.PG_CATALOG.equalsIgnoreCase(schema)
                || Consts.INFORMATION_SCHEMA.equalsIgnoreCase(schema);
    }

    /**
     * Checks if a schema is a Microsoft SQL Server system schema.
     *
     * @param schema the schema name to check
     * @return true if the schema is 'sys', false otherwise
     */
    public static boolean isMsSystemSchema(String schema) {
        return Consts.SYS.equalsIgnoreCase(schema);
    }

    /**
     * Checks if a string contains any of the items in a list.
     *
     * @param input the string to search
     * @param items the list of strings to search for
     * @return true if any item is found in the input string, false otherwise
     */
    public static boolean stringContainsAnyItem(String input, List<String> items) {
        return items.stream().anyMatch(input::contains);
    }

    /**
     * Processes newlines in a string according to the specified setting.
     *
     * @param text           the input text
     * @param isKeepNewlines if true, preserves newlines; if false, removes carriage returns
     * @return the processed string
     */
    public static String checkNewLines(String text, boolean isKeepNewlines) {
        return isKeepNewlines ? text : text.replace("\r", "");
    }

    /**
     * Gets the implementation version of the core package.
     *
     * @return the version string, or "unknown" if not available
     */
    public static String getVersion() {
        var coreVer = Utils.class.getPackage().getImplementationVersion();
        if (coreVer == null) {
            coreVer = "unknown";
        }
        return coreVer;
    }

    /**
     * Casts a Stream into Iterable. Stream is consumed after Iterable.iterator() is called.
     */
    public static <T> Iterable<T> streamIterator(Stream<T> stream) {
        return stream::iterator;
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
    public static String getErrorSubstring(String s, int pos) {
        return getErrorSubstring(s, pos, ERROR_SUBSTRING_LENGTH);
    }

    /**
     * Gets error substring from specified position with custom length.
     *
     * @param s   the string to extract from
     * @param pos the starting position
     * @param len the maximum length of substring
     * @return substring of error text
     */
    public static String getErrorSubstring(String s, int pos, int len) {
        if (pos >= s.length()) {
            return "";
        }
        return pos + len < s.length() ? s.substring(pos, pos + len) : s.substring(pos);
    }

    /**
     * Compares 2 collections for equality unorderedly as if they were {@link Set}s.<br>
     * Does not eliminate duplicate elements as sets do and counts them instead. Thus it achieves complementarity with
     * setLikeHashcode while not requiring it to eliminate duplicates, nor does it require a
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
    public static boolean setLikeEquals(Collection<?> c1, Collection<?> c2) {
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


    public static Random getRandom() {
        return RANDOM;
    }

    private Utils() {
    }
}
