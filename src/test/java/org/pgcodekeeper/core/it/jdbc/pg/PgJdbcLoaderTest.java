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
package org.pgcodekeeper.core.it.jdbc.pg;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.pgcodekeeper.core.settings.CoreSettings;

class PgJdbcLoaderTest extends AbstractPgGpJdbcLoaderTest {

    @ParameterizedTest
    @CsvSource({
            "dump_test, PG_16",
            "operator, PG_16",
            "statistics, PG_16",
            "view, PG_16",
            "not_null, PG_18",
    })
    void jdbcLoaderTest(String fileName, String contTypeName) throws Exception {
        var settings = new CoreSettings();
        settings.setEnableFunctionBodiesDependencies(true);
        jdbcLoaderTest(false, fileName, contTypeName, settings);
    }

    @ParameterizedTest
    @CsvSource({
            "view, PG_16",
            "not_null, PG_18"
    })
    void jdbcLoaderSpecialTest(String fileName, String contTypeName) throws Exception {
        var settings = new CoreSettings();
        settings.setSimplifyNotNull(true);
        settings.setSimplifyView(true);
        jdbcLoaderTest(true, fileName, contTypeName, settings);
    }
}
