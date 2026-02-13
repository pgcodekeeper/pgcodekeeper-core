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
package org.pgcodekeeper.core.model.graph.ch;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.pgcodekeeper.core.FILES_POSTFIX;
import org.pgcodekeeper.core.TestUtils;
import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.ch.ChDatabaseProvider;
import org.pgcodekeeper.core.it.IntegrationTestUtils;
import org.pgcodekeeper.core.model.graph.DepcyFinder;
import org.pgcodekeeper.core.settings.CoreSettings;

class ChDepcyFinderTest {

    @ParameterizedTest
    @CsvSource({
            "ch_view, default.revenue0",
            "ch_view_2, default.TestView",
            "ch_view_3, default.goal_view",
            "ch_view_4, default.view_join_inner_table",
            "ch_view_5, default.view_aaa_bbb",
            "ch_view_6, default.view_columns_transformers",
            "ch_view_7, default.ch_view_7",
            "ch_dictionary, default.dict",
    })
    void compareReverseGraph(String fileName, String objectName) throws IOException, InterruptedException {
        compareGraph(fileName, FILES_POSTFIX.DEPS_REVERSE_TXT, objectName, true);
    }

    void compareGraph(String fileName, FILES_POSTFIX expectedPostfix, String objectName, boolean isReverse)
            throws IOException, InterruptedException {
        var settings = new CoreSettings();
        ChDatabaseProvider databaseProvider = new ChDatabaseProvider();

        settings.setEnableFunctionBodiesDependencies(true);

        IDatabase db = IntegrationTestUtils.loadTestDump(databaseProvider, fileName + FILES_POSTFIX.SQL, getClass(), settings);

        var deps = DepcyFinder.byPatterns(10, isReverse, Collections.emptyList(), false, db, List.of(objectName));
        String actual = String.join("\n", deps);
        String expected = TestUtils.readResource(fileName + expectedPostfix, getClass());

        TestUtils.assertIgnoreNewLines(expected.trim(), actual);
    }
}
