package ru.taximaxim.codekeeper.core.parsers.antlr;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class QNameParserTest {

    private static final String SCHEMA = "schema";
    private static final String TABLE = "table";
    private static final Object COLUMN = "column";

    @Test
    public void testParseSchemaBothQuoted() {
        Assertions.assertEquals(SCHEMA, QNameParser.parsePg("\"schema\".\"table\"").getSchemaName());
    }

    @Test
    public void testParseSchemaFirstQuoted() {
        Assertions.assertEquals(SCHEMA, QNameParser.parsePg("\"schema\".table").getSchemaName());
    }

    @Test
    public void testParseSchemaSecondQuoted() {
        Assertions.assertEquals(SCHEMA, QNameParser.parsePg("schema.\"table\"").getSchemaName());
    }

    @Test
    public void testParseSchemaNoneQuoted() {
        Assertions.assertEquals(SCHEMA, QNameParser.parsePg("schema.table").getSchemaName());
    }

    @Test
    public void testParseSchemaThreeQuoted() {
        Assertions.assertEquals(SCHEMA, QNameParser.parsePg("\"schema\".\"table\".\"column\"").getSchemaName());
    }

    @Test
    public void testParseObjectBothQuoted() {
        Assertions.assertEquals(TABLE, QNameParser.parsePg("\"schema\".\"table\"").getFirstName());
    }

    public void testParseObjectFirstQuoted() {
        Assertions.assertEquals(TABLE, QNameParser.parsePg("\"schema\".table").getFirstName());
    }

    @Test
    public void testParseObjectSecondQuoted() {
        Assertions.assertEquals(TABLE, QNameParser.parsePg("schema.\"table\"").getFirstName());
    }

    @Test
    public void testParseObjectNoneQuoted() {
        Assertions.assertEquals(TABLE, QNameParser.parsePg("schema.table").getFirstName());
    }

    @Test
    public void testParseObjectThreeQuoted() {
        Assertions.assertEquals(COLUMN, QNameParser.parsePg("\"schema\".\"table\".\"column\"").getFirstName());
    }
}
