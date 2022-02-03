package cz.startnet.utils.pgdiff.xmlstore;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.List;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import cz.startnet.utils.pgdiff.libraries.PgLibrary;

/**
 * Library XML list read/write class.<br>
 * Do not call {@link #writeObjects(List)} on this class,
 * doing that won't save additional library options.<br>
 * Call {@link #writeDependencies(List, boolean)} instead.
 */
public class DependenciesXmlStore extends XmlStore<PgLibrary> {

    public static final String FILE_NAME = ".dependencies"; //$NON-NLS-1$

    private static final String ENTRY = "dependency"; //$NON-NLS-1$
    private static final String PATH = "path"; //$NON-NLS-1$
    private static final String OWNER = "owner"; //$NON-NLS-1$
    private static final String IGNORE_PRIV = "ignorePriv"; //$NON-NLS-1$
    private static final String ROOT_TAG = "dependencies"; //$NON-NLS-1$
    private static final String LOAD_NESTED = "loadNested"; //$NON-NLS-1$

    private final Path xmlPath;

    public DependenciesXmlStore(Path path) {
        super(path.getFileName().toString(), ROOT_TAG);
        this.xmlPath = path;
    }

    @Override
    protected Path getXmlFile() {
        return xmlPath;
    }

    public boolean readLoadNestedFlag() throws IOException {
        try {
            return Boolean.parseBoolean(readXml(true).getDocumentElement().getAttribute(LOAD_NESTED));
        } catch (NoSuchFileException ex) {
            return false;
        }
    }

    public void writeDependencies(List<PgLibrary> depdencies, boolean loadNestedFlag) throws IOException {
        Document xml = createDocument(depdencies);
        xml.getDocumentElement().setAttribute(LOAD_NESTED, Boolean.toString(loadNestedFlag));
        writeDocument(xml);
    }

    @Override
    protected PgLibrary parseElement(Node node) {
        NamedNodeMap attr = node.getAttributes();
        Node owner = attr.getNamedItem(OWNER);
        return new PgLibrary(attr.getNamedItem(PATH).getTextContent(),
                Boolean.parseBoolean(attr.getNamedItem(IGNORE_PRIV).getTextContent()),
                owner == null ? "" : owner.getTextContent());
    }

    @Override
    protected void appendChildren(Document xml, Element root, List<PgLibrary> list) {
        for (PgLibrary lib : list) {
            Element newElement = xml.createElement(ENTRY);
            newElement.setAttribute(IGNORE_PRIV, Boolean.toString(lib.isIgnorePriv()));
            newElement.setAttribute(PATH, lib.getPath());
            newElement.setAttribute(OWNER, lib.getOwner());

            root.appendChild(newElement);
        }
    }
}
