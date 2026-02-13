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
package org.pgcodekeeper.core.model.graph.pg;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.pgcodekeeper.core.FILES_POSTFIX;
import org.pgcodekeeper.core.TestUtils;
import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.pg.PgDatabaseProvider;
import org.pgcodekeeper.core.it.IntegrationTestUtils;
import org.pgcodekeeper.core.model.graph.DepcyFinder;
import org.pgcodekeeper.core.settings.CoreSettings;

class PgDepcyFinderTest {

    @ParameterizedTest
    @CsvSource({
            // test analysis work on merge statement
            "function_merge, public.f1",
            "view, public.v1",
    })
    void compareReverseGraph(String fileName, String objectName) throws IOException, InterruptedException {
        compareGraph(fileName, FILES_POSTFIX.DEPS_REVERSE_TXT, objectName, true);
    }

    @ParameterizedTest
    @CsvSource({
            "table, public.t1",
            "regex, public\\.t.",
            //test searching deps of object "MyTable" by name without quotes and case insensitive mode for PG
            "quotes, public.mytable",
            //test searching deps of object "MyTable" by name with quotes for PG
            "quotes, public.\"mytable\"",
            // test searching deps of function by full name
            "function_circle, public.f1\\(\\)",
            //test searching deps of function by name with regex for PG
            "function_circle, 'public\\.f1\\(.*'",
            //test searching deps of function by name without parens for PG
            "function_circle, 'public\\.f1'",
            //test searching deps of function by name with quotes and without parens for PG
            "function_circle_quotes, 'public\\.\"Func1\"\\(.*'",
            //test searching deps of quoted function by name without quotes and parens for PG
            "function_circle_quotes, 'public\\.func1'",
            //test searching deps of table constraint
            "table_constraint, public.test_fk_1.fk",
    })
    void compareBothGraph(String fileName, String objectName) throws IOException, InterruptedException {
        compareGraph(fileName, FILES_POSTFIX.DEPS_TXT, objectName, false);
        compareGraph(fileName, FILES_POSTFIX.DEPS_REVERSE_TXT, objectName, true);
    }

    void compareGraph(String fileName, FILES_POSTFIX expectedPostfix, String objectName, boolean isReverse)
            throws IOException, InterruptedException {
        var settings = new CoreSettings();
        PgDatabaseProvider databaseProvider = new PgDatabaseProvider();

        settings.setEnableFunctionBodiesDependencies(true);

        IDatabase db = IntegrationTestUtils.loadTestDump(databaseProvider, fileName + FILES_POSTFIX.SQL, getClass(), settings);

        var deps = DepcyFinder.byPatterns(10, isReverse, Collections.emptyList(), false, db, List.of(objectName));
        String actual = String.join("\n", deps);
        String expected = TestUtils.readResource(fileName + expectedPostfix, getClass());

        TestUtils.assertIgnoreNewLines(expected.trim(), actual);
    }
}
