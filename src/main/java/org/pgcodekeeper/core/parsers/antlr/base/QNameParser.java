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
package org.pgcodekeeper.core.parsers.antlr.base;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.pgcodekeeper.core.parsers.antlr.ch.statement.ChParserAbstract;
import org.pgcodekeeper.core.parsers.antlr.pg.statement.PgParserAbstract;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Utility class for parsing and processing qualified names (schema.object names)
 * in SQL statements. Supports extracting individual name components from
 * schema-qualified identifiers.
 *
 * @param <T> the type of parser context being processed
 */
public final class QNameParser<T extends ParserRuleContext> {

    /**
     * Gets the first (leftmost) name component from a list of identifiers.
     *
     * @param ids list of identifier contexts
     * @return first name component or null if not available
     */
    public static <T extends ParserRuleContext> String getFirstName(List<T> ids) {
        return getLastId(ids, 1);
    }

    /**
     * Gets the second name component from a list of identifiers.
     *
     * @param ids list of identifier contexts
     * @return second name component or null if not available
     */
    public static <T extends ParserRuleContext> String getSecondName(List<T> ids) {
        return getLastId(ids, 2);
    }

    /**
     * Gets the third name component from a list of identifiers.
     *
     * @param ids list of identifier contexts
     * @return third name component or null if not available
     */
    public static <T extends ParserRuleContext> String getThirdName(List<T> ids) {
        return getLastId(ids, 3);
    }

    /**
     * Gets the context for the first name component.
     *
     * @param ids list of identifier contexts
     * @return parser context for first name or null
     */
    public static <T extends ParserRuleContext> T getFirstNameCtx(List<T> ids) {
        return getLastIdCtx(ids, 1);
    }

    /**
     * Gets the context for the second name component.
     *
     * @param ids list of identifier contexts
     * @return parser context for second name or null
     */
    public static <T extends ParserRuleContext> T getSecondNameCtx(List<T> ids) {
        return getLastIdCtx(ids, 2);
    }

    /**
     * Gets the context for the third name component.
     *
     * @param ids list of identifier contexts
     * @return parser context for third name or null
     */
    public static <T extends ParserRuleContext> T getThirdNameCtx(List<T> ids) {
        return getLastIdCtx(ids, 3);
    }

    /**
     * Gets the schema name from a qualified identifier.
     *
     * @param ids list of identifier contexts
     * @return schema name or null if not qualified
     */
    public static <T extends ParserRuleContext> String getSchemaName(List<T> ids) {
        ParserRuleContext schemaCtx = getSchemaNameCtx(ids);
        return schemaCtx == null ? null : getText(schemaCtx);
    }

    /**
     * Gets the context for the schema name component.
     *
     * @param ids list of identifier contexts
     * @return parser context for schema name or null
     */
    public static <T extends ParserRuleContext> T getSchemaNameCtx(List<T> ids) {
        return ids.size() < 2 ? null : ids.get(0);
    }

    private static <T extends ParserRuleContext> String getLastId(List<T> ids, int i) {
        ParserRuleContext ctx = getLastIdCtx(ids, i);
        return ctx == null ? null : getText(ctx);
    }

    private static <T extends ParserRuleContext> T getLastIdCtx(List<T> ids, int i) {
        int n = ids.size() - i;
        return n < 0 ? null : ids.get(n);
    }

    private static String getText(ParserRuleContext ctx) {
        List<ParseTree> children = ctx.children;
        while (children != null) {
            if (children.size() == 1) {
                ParseTree tree = children.get(0);
                if (tree instanceof ParserRuleContext ruleCtx) {
                    children = ruleCtx.children;
                } else if (tree instanceof TerminalNode) {
                    return tree.getText();
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        return ctx.getText();
    }

    private final List<T> parts;
    private final List<Object> errors;

    public List<T> getIds() {
        return Collections.unmodifiableList(parts);
    }

    /**
     * Parses a PostgreSQL qualified name into its components.
     *
     * @param schemaQualifiedName the qualified name string to parse
     * @return QNameParser instance containing parsed components
     */
    public static QNameParser<ParserRuleContext> parsePg(String schemaQualifiedName) {
        List<Object> errors = new ArrayList<>();
        var parser = AntlrParser.createSQLParser(schemaQualifiedName, "qname: " + schemaQualifiedName, errors);
        var parts = PgParserAbstract.getIdentifiers(parser.qname_parser().schema_qualified_name());
        return new QNameParser<>(parts, errors);
    }

    /**
     * Parses a ClickHouse qualified name into its components.
     *
     * @param schemaQualifiedName the qualified name string to parse
     * @return QNameParser instance containing parsed components
     */
    public static QNameParser<ParserRuleContext> parseCh(String schemaQualifiedName) {
        List<Object> errors = new ArrayList<>();
        var parser = AntlrParser.createCHParser(schemaQualifiedName, "qname: " + schemaQualifiedName, errors);
        var parts = ChParserAbstract.getIdentifiers(parser.qname_parser().qualified_name());
        return new QNameParser<>(parts, errors);
    }

    /**
     * Parses a PostgreSQL operator name into its components.
     *
     * @param schemaQualifiedName the operator name string to parse
     * @return QNameParser instance containing parsed components
     */
    public static QNameParser<ParserRuleContext> parsePgOperator(String schemaQualifiedName) {
        List<Object> errors = new ArrayList<>();
        var parser = AntlrParser.createSQLParser(schemaQualifiedName, "qname: " + schemaQualifiedName, errors);
        var parts = PgParserAbstract.getIdentifiers(parser.operator_args_parser().operator_name());
        return new QNameParser<>(parts, errors);
    }

    private QNameParser(List<T> parts, List<Object> errors) {
        this.errors = errors;
        this.parts = parts;
    }

    /**
     * Gets the first name component from this qualified name.
     *
     * @return first name component or null
     */
    public String getFirstName() {
        return getFirstName(parts);
    }

    /**
     * Gets the second name component from this qualified name.
     *
     * @return second name component or null
     */
    public String getSecondName() {
        return getSecondName(parts);
    }

    /**
     * Gets the third name component from this qualified name.
     *
     * @return third name component or null
     */
    public String getThirdName() {
        return getThirdName(parts);
    }

    /**
     * Gets the schema name from this qualified name.
     *
     * @return schema name or null if not qualified
     */
    public String getSchemaName() {
        return getSchemaName(parts);
    }

    /**
     * Checks if any errors occurred during parsing.
     *
     * @return true if parsing errors were encountered
     */
    public boolean hasErrors() {
        return !errors.isEmpty();
    }
}
