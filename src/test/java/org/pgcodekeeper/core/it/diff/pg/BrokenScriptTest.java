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
package org.pgcodekeeper.core.it.diff.pg;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.pgcodekeeper.core.FILES_POSTFIX;
import org.pgcodekeeper.core.loader.PgDumpLoader;
import org.pgcodekeeper.core.settings.CoreSettings;

import java.io.IOException;

class BrokenScriptTest {

    @ParameterizedTest
    @CsvSource({
            "broken_duplicated_schema, line 3:1 SCHEMA test already exists",
            "broken_duplicated_index, line 5:1 INDEX i already exists for TABLE t1"
    })
    void brokenScriptTest(String fileNameTemplate, String expectedError) throws IOException, InterruptedException {
        var settings = new CoreSettings();

        String resource = fileNameTemplate + FILES_POSTFIX.SQL;
        PgDumpLoader loader = new PgDumpLoader(() -> getClass().getResourceAsStream(resource), resource, settings);
        loader.loadAndAnalyze();
        var errors = loader.getErrors();

        Assertions.assertEquals(1, errors.size());

        String actualError = errors.get(0).toString();

        if (!actualError.endsWith(expectedError)) {
            Assertions.assertEquals(expectedError, actualError);
        }
    }
}
