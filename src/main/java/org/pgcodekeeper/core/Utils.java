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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.UnaryOperator;

/**
 * Utility class providing common helper methods for various operations including
 * serialization, XML parsing, schema validation, and string manipulation.
 */
public final class Utils {

    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

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
     * @param inputStr the string to search
     * @param items    the list of strings to search for
     * @return true if any item is found in the input string, false otherwise
     */
    public static boolean stringContainsAnyItem(String inputStr, List<String> items) {
        return items.stream().anyMatch(inputStr::contains);
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

    private Utils() {
    }
}
