package ru.taximaxim.codekeeper.core.parsers.antlr.expr;

public interface TypesSetManually {
    String UNKNOWN = "unknown_unknown";
    String EMPTY = "empty";

    String COLUMN = "column";
    String FUNCTION_COLUMN = "functionCol";
    String FUNCTION_TABLE = "functionTable";

    String QUALIFIED_ASTERISK = "qualifiedAsterisk";

    String BIT = "bit";
    String BOOLEAN = "boolean";
    String INTEGER = "integer";
    String NUMERIC = "numeric";
    String DOUBLE = "double precision";
    String BPCHAR = "bpchar";
    String TEXT = "text";
    String NAME = "name";
    String XML = "xml";
    String ANY = "any";
    String ANYTYPE = "anyelement";
    String ANYARRAY = "anyarray";
    String ANYENUM = "anyenum";
    String ANYRANGE = "anyrange";
    String ANYNOARRAY = "anynonarray";

    String DATE = "date";
    String TIMETZ = "time with time zone";
    String TIMESTAMPTZ = "timestamp with time zone";
    String TIME = "time without time zone";
    String TIMESTAMP = "timestamp without time zone";
    String CURSOR = "refcursor";
}
