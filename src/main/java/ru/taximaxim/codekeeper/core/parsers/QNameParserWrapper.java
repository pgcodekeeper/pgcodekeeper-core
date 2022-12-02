package ru.taximaxim.codekeeper.core.parsers;

import ru.taximaxim.codekeeper.core.parsers.antlr.QNameParser;

/**
 * Wrapper for QNameParser to hide ANTLR context
 *
 * @author galiev_mr
 *
 * @since 8.0.1
 */
public class QNameParserWrapper {

    private final QNameParser<?> parser;

    /**
     * parse PostgreSQL qualified name
     *
     * @param fullName
     *            - full object name for parse
     * @return parser wrapper
     *
     * @since 8.0.1
     */
    public static QNameParserWrapper parsePg(String fullName) {
        return new QNameParserWrapper(QNameParser.parsePg(fullName));
    }

    /**
     * Checks parser errors
     *
     * @return true if there were errors during parsing
     *
     * @since 8.0.1
     */
    public boolean hasErrors() {
        return parser.hasErrors();
    }

    /**
     * @return first part of qualified name
     *
     * @since 8.0.1
     */
    public String getFirstName() {
        return parser.getFirstName();
    }

    /**
     * @return second part of qualified name or null if not exists
     *
     * @since 8.0.1
     */
    public String getSecondName() {
        return parser.getSecondName();
    }

    /**
     * @return third part of qualified name or null if not exists
     *
     * @since 8.0.1
     */
    public String getThirdName() {
        return parser.getThirdName();
    }

    private QNameParserWrapper(QNameParser<?> parser) {
        this.parser = parser;
    }
}
