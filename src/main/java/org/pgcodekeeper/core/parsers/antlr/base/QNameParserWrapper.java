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
package org.pgcodekeeper.core.parsers.antlr.base;

/**
 * Wrapper class that provides simplified access
 * to parsed qualified name components without exposing the underlying parser implementation.
 * Handles both PostgreSQL and ClickHouse qualified name formats.
 */
public class QNameParserWrapper {

    private final QNameParser<?> parser;

    private QNameParserWrapper(QNameParser<?> parser) {
        this.parser = parser;
    }

    /**
     * Creates a wrapper for parsing PostgreSQL qualified names.
     *
     * @param fullName the qualified name string to parse (e.g. "schema.table")
     * @return wrapper containing parsed name components
     */
    public static QNameParserWrapper parsePg(String fullName) {
        return new QNameParserWrapper(QNameParser.parsePg(fullName));
    }

    /**
     * Creates a wrapper for parsing ClickHouse qualified names.
     *
     * @param fullName the qualified name string to parse
     * @return wrapper containing parsed name components
     */
    public static QNameParserWrapper parseCh(String fullName) {
        return new QNameParserWrapper(QNameParser.parseCh(fullName));
    }

    /**
     * Creates a wrapper for parsing PostgreSQL operator names.
     *
     * @param fullName the operator name string to parse
     * @return wrapper containing parsed name components
     */
    public static QNameParserWrapper parsePgOperator(String fullName) {
        return new QNameParserWrapper(QNameParser.parsePgOperator(fullName));
    }

    public String getFirstName() {
        return parser.getFirstName();
    }

    public String getSecondName() {
        return parser.getSecondName();
    }

    public String getSchemaName() {
        return parser.getSchemaName();
    }

    public String getThirdName() {
        return parser.getThirdName();
    }

    public boolean hasErrors() {
        return parser.hasErrors();
    }
}
