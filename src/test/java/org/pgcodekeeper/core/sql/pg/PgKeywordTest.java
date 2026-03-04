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
package org.pgcodekeeper.core.sql.pg;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pgcodekeeper.core.database.pg.utils.PgKeyword;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

class PgKeywordTest {

    @ParameterizedTest
    @ValueSource(strings = {"alter", "extension", "merge", "name", "owner", "skip"})
    @DisplayName("Test unreserved keywords")
    void testUnreservedKeywordsKeywords(String keyword) {
        assertFalse(PgKeyword.isKeyword(keyword), "This is unreserved keyword");
    }

    @ParameterizedTest
    @ValueSource(strings = {"tbl", "423df", "public", "we"})
    @DisplayName("Test random words")
    void testRandomId(String keyword) {
        assertFalse(PgKeyword.isKeyword(keyword), "This is no keyword");
    }

    @ParameterizedTest
    @ValueSource(strings = {"all", "case", "between", "window", "collation"})
    @DisplayName("Test reserved keywords")
    void testKeywords(String keyword) {
        assertTrue(PgKeyword.isKeyword(keyword), "This is reserved keyword");
    }
}
