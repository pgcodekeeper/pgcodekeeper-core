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
package org.pgcodekeeper.core.database.base.parser;

/**
 * Wrapper class that provides simplified access
 * to parsed qualified name components without exposing the underlying parser implementation.
 * Handles both PostgreSQL and ClickHouse qualified name formats.
 */
public class QNameParserWrapper {

    private final QNameParser<?> parser;

    public QNameParserWrapper(QNameParser<?> parser) {
        this.parser = parser;
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
