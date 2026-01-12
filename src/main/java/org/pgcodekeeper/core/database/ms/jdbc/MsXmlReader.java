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
package org.pgcodekeeper.core.database.ms.jdbc;

import org.pgcodekeeper.core.exception.XmlReaderException;
import org.pgcodekeeper.core.utils.Utils;
import org.w3c.dom.*;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * XML reader for parsing Microsoft SQL Server XML query results.
 * Provides typed access to XML attributes converted from database query results.
 */
public class MsXmlReader {

    private static final String ROOT = "root";

    private final Map<String, String> result;

    private MsXmlReader(Map<String, String> result) {
        this.result = result;
    }

    /**
     * Returns the double value for the specified column name.
     *
     * @param columnName the column name
     * @return the double value, or 0 if null
     */
    public double getDouble(String columnName) {
        String val = result.get(columnName);
        return val == null ? 0 : Double.parseDouble(val);
    }

    /**
     * Returns the long value for the specified column name.
     *
     * @param columnName the column name
     * @return the long value, or 0 if null
     */
    public long getLong(String columnName) {
        String val = result.get(columnName);
        return val == null ? 0 : Long.parseLong(val);
    }

    /**
     * Returns the boolean value for the specified column name.
     * Returns true if the value equals "1".
     *
     * @param columnName the column name
     * @return true if the value is "1", false otherwise
     */
    public boolean getBoolean(String columnName) {
        String o = result.get(columnName);
        return "1".equals(o);
    }

    /**
     * Returns the string value for the specified column name.
     *
     * @param columnName the column name
     * @return the string value, or null if not found
     */
    public String getString(String columnName) {
        return result.get(columnName);
    }

    /**
     * Returns the float value for the specified column name.
     *
     * @param columnName the column name
     * @return the float value, or 0 if null
     */
    public float getFloat(String columnName) {
        String val = result.get(columnName);
        return val == null ? 0 : Float.parseFloat(val);
    }

    /**
     * Returns the integer value for the specified column name.
     *
     * @param columnName the column name
     * @return the integer value, or 0 if null
     */
    public int getInt(String columnName) {
        String val = result.get(columnName);
        return val == null ? 0 : Integer.parseInt(val);
    }

    /**
     * Returns the short value for the specified column name.
     *
     * @param columnName the column name
     * @return the short value, or 0 if null
     */
    public short getShort(String columnName) {
        String val = result.get(columnName);
        return val == null ? 0 : Short.parseShort(val);
    }

    /**
     * Parses XML string into a list of XmlReader instances.
     * Each child element of the root becomes an XmlReader with its attributes as accessible data.
     *
     * @param xml the XML string to parse
     * @return list of XmlReader instances, one for each child element
     * @throws XmlReaderException if XML parsing fails
     */
    public static List<MsXmlReader> readXML(String xml) throws XmlReaderException {
        if (xml == null) {
            return new ArrayList<>();
        }

        try (Reader reader = new InputStreamReader(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)))) {
            Document doc = Utils.readXml(reader);

            if (!ROOT.equals(doc.getDocumentElement().getNodeName())) {
                throw new IOException("XML root element name is not as requested.");
            }

            Element root = (Element) doc.getElementsByTagName(ROOT).item(0);
            NodeList nList = root.getChildNodes();
            int size = nList.getLength();
            List<MsXmlReader> readers = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                Node node = nList.item(i);
                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    readers.add(parseElement(node));
                }
            }

            return readers;
        } catch (IOException | SAXException | ParserConfigurationException ex) {
            throw new XmlReaderException(ex.getLocalizedMessage(), ex);
        }
    }

    private static MsXmlReader parseElement(Node node) {
        NamedNodeMap params = node.getAttributes();
        int size = params.getLength();
        Map<String, String> attr = new HashMap<>(size);

        for (int i = 0; i < size; i++) {
            Node param = params.item(i);
            if (param.getNodeType() == Node.ATTRIBUTE_NODE) {
                attr.put(param.getNodeName(), param.getTextContent());
            }
        }

        return new MsXmlReader(attr);
    }
}