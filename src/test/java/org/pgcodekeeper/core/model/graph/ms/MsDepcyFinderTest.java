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
package org.pgcodekeeper.core.model.graph.ms;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.pgcodekeeper.core.FILES_POSTFIX;
import org.pgcodekeeper.core.TestUtils;
import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.ms.MsDatabaseProvider;
import org.pgcodekeeper.core.it.IntegrationTestUtils;
import org.pgcodekeeper.core.model.graph.DepcyFinder;
import org.pgcodekeeper.core.settings.CoreSettings;

class MsDepcyFinderTest {

    @ParameterizedTest
    @CsvSource({
            // test analysis work on window statement
            "ms_view_window, dbo.v1",
            // "ms_insert_name, public.sales5",
            "ms_procedure, \\[dbo\\].\\[test_poc\\]",
            // same test searching deps of object by name without quotes for MS
            "ms_procedure, dbo.test_poc",
            // test searching deps of object by name without quotes and case insensitive mode for MS
            "ms_UPPER_CASE, dbo.TEST_POC",
    })
    void compareReverseGraph(String fileName, String objectName) throws IOException, InterruptedException {
        compareGraph(fileName, FILES_POSTFIX.DEPS_REVERSE_TXT, objectName, true);
    }

    @ParameterizedTest
    @CsvSource({
            "ms_sys_ver_table, '\\[dbo\\]\\.\\[t1\\]'",
    })
    void compareBothGraph(String fileName, String objectName) throws IOException, InterruptedException {
        compareGraph(fileName, FILES_POSTFIX.DEPS_TXT, objectName, false);
        compareGraph(fileName, FILES_POSTFIX.DEPS_REVERSE_TXT, objectName, true);
    }

    void compareGraph(String fileName, FILES_POSTFIX expectedPostfix, String objectName, boolean isReverse)
            throws IOException, InterruptedException {
        var settings = new CoreSettings();
        MsDatabaseProvider databaseProvider = new MsDatabaseProvider();

        settings.setEnableFunctionBodiesDependencies(true);

        IDatabase db = IntegrationTestUtils.loadTestDump(databaseProvider, fileName + FILES_POSTFIX.SQL, getClass(), settings);

        var deps = DepcyFinder.byPatterns(10, isReverse, Collections.emptyList(), false, db, List.of(objectName));
        String actual = String.join("\n", deps);
        String expected = TestUtils.readResource(fileName + expectedPostfix, getClass());

        TestUtils.assertIgnoreNewLines(expected.trim(), actual);
    }
}
