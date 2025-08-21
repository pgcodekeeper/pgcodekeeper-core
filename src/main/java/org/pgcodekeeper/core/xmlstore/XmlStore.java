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
package org.pgcodekeeper.core.xmlstore;

import org.pgcodekeeper.core.Utils;
import org.pgcodekeeper.core.localizations.Messages;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Abstract base class for XML-based object storage and retrieval.
 * Provides common functionality for reading and writing collections of objects
 * to XML files with proper validation and error handling.
 *
 * @param <T> the type of objects stored in this XML store
 */
public abstract class XmlStore<T> {

    protected final String fileName;
    private final String rootTag;

    private Document cachedDocument;

    protected XmlStore(String fileName, String rootTag) {
        this.fileName = fileName;
        this.rootTag = rootTag;
    }

    protected Element createSubElement(Document xml, Element parent, String name, String value) {
        Element newElement = xml.createElement(name);
        newElement.setTextContent(value);
        parent.appendChild(newElement);
        return newElement;
    }

    protected abstract Path getXmlFile();

    /**
     * Reads all objects from the XML file.
     *
     * @return list of objects read from XML, empty list if file doesn't exist
     * @throws IOException if reading fails
     */
    public List<T> readObjects() throws IOException {
        return getObjects(readXml(false));
    }

    protected List<T> getObjects(Document xml) {
        List<T> objects = new ArrayList<>();
        Element root = (Element) xml.getElementsByTagName(rootTag).item(0);
        NodeList nList = root.getChildNodes();
        for (int i = 0; i < nList.getLength(); i++) {
            Node node = nList.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE) {
                objects.add(parseElement(node));
            }
        }
        return objects;
    }

    protected abstract T parseElement(Node node);

    /**
     * Writes objects to the XML file.
     *
     * @param list the list of objects to write
     * @throws IOException if writing fails
     */
    public void writeObjects(List<T> list) throws IOException {
        writeDocument(createDocument(list));
    }

    protected Document createDocument(List<T> list) throws IOException {
        try {
            Document xml = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
            Element root = xml.createElement(rootTag);
            xml.appendChild(root);
            appendChildren(xml, root, list);
            return xml;
        } catch (ParserConfigurationException ex) {
            throw new IOException(MessageFormat.format(
                    Messages.XmlStore_write_error, ex.getLocalizedMessage()), ex);
        }
    }

    /**
     * Writes an XML document to a file
     *
     * @param xml - {@link Document} witch store xml document
     * @throws IOException if an I/O error occurs
     */
    protected void writeDocument(Document xml) throws IOException {
        try {
            Path path = getXmlFile();
            Files.createDirectories(path.getParent());
            writeDocument(xml, path);
        } catch (Exception ex) {
            throw new IOException(MessageFormat.format(
                    Messages.XmlStore_write_error, ex.getLocalizedMessage()), ex);
        }
    }

    /**
     * Writes an XML document to a file
     *
     * @param xml  - {@link Document} witch store xml document
     * @param path - Path to the file where the XML document will be written
     * @throws IOException          if an I/O error occurs
     * @throws TransformerException if an error occurred during the XML transformation
     */
    protected void writeDocument(Document xml, Path path) throws IOException, TransformerException {
        try (Writer xmlWriter = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            Utils.writeXml(xml, false, new StreamResult(xmlWriter));
        }
    }

    protected abstract void appendChildren(Document xml, Element root, List<T> list);

    /**
     * Reads (well-formed) list XML and checks it for basic validity:
     * root node must be <code>&lt;rootTagName&gt;</code>
     *
     * @param useCached immediately return the Document read in the previous call to this method
     * @return the parsed XML document
     * @throws IOException if reading or parsing fails
     */
    protected synchronized Document readXml(boolean useCached) throws IOException {
        if (useCached && cachedDocument != null) {
            return cachedDocument;
        }
        Document xml = readXml();
        if (!xml.getDocumentElement().getNodeName().equals(rootTag)) {
            throw new IOException(Messages.XmlStore_root_error);
        }
        cachedDocument = xml;
        return xml;
    }

    /**
     * Reads and parses an XML document from a file
     *
     * @return - {@link Document} witch store xml document
     * @throws IOException if reading or parsing fails
     */
    protected Document readXml() throws IOException {
        var xmlPath = getXmlFile();
        if (!Files.exists(xmlPath)) {
            return createDocument(Collections.emptyList());
        }
        try (Reader reader = Files.newBufferedReader(xmlPath, StandardCharsets.UTF_8)) {
            return Utils.readXml(reader);
        } catch (Exception e) {
            throw new IOException(MessageFormat.format(Messages.XmlStore_read_error, e.getLocalizedMessage()), e);
        }
    }
}
