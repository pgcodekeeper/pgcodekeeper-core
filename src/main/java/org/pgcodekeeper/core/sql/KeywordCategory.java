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
package org.pgcodekeeper.core.sql;

/**
 * Keyword categories
 */
public enum KeywordCategory {

    UNRESERVED_KEYWORD("tokens_nonreserved"),
    COL_NAME_KEYWORD("tokens_nonreserved_except_function_type"),
    TYPE_FUNC_NAME_KEYWORD("tokens_reserved_except_function_type"),
    RESERVED_KEYWORD("tokens_reserved");

    private final String parserRule;

    KeywordCategory(String parserRule) {
        this.parserRule = parserRule;
    }

    public String getParserRule() {
        return parserRule;
    }
}
