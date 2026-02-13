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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pgcodekeeper.core.FILES_POSTFIX;
import org.pgcodekeeper.core.TestUtils;
import org.pgcodekeeper.core.api.PgCodeKeeperApi;
import org.pgcodekeeper.core.it.IntegrationTestUtils;
import org.pgcodekeeper.core.monitor.NullMonitor;
import org.pgcodekeeper.core.database.pg.PgDatabaseProvider;
import org.pgcodekeeper.core.database.pg.loader.PgLibraryLoader;
import org.pgcodekeeper.core.database.pg.schema.PgDatabase;
import org.pgcodekeeper.core.settings.CoreSettings;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

class LibDiffTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "lib_test_with_ignore",
            "no_same_objects",
            "with_same_objects"
    })
    void diffWithIgnoreTest(String fileNameTemplate) throws IOException, InterruptedException, URISyntaxException {
        testLibrary(fileNameTemplate, List.of(fileNameTemplate + FILES_POSTFIX.LIBRARY_SQL), true);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "lib_test",
            "no_same_objects",
            "with_same_objects"
    })
    void diffNoIgnoreTest(String fileNameTemplate) throws IOException, InterruptedException, URISyntaxException {
        testLibrary(fileNameTemplate, List.of(fileNameTemplate + FILES_POSTFIX.LIBRARY_SQL), false);
    }

    @Test
    void diffMultipleLibTest() throws IOException, InterruptedException, URISyntaxException {
        List<String> libList = List.of("multiple_libs_lib1.sql", "multiple_libs_lib2.sql", "multiple_libs_lib3.sql");
        testLibrary("multiple_libs", libList, true);
    }

    private void testLibrary(String fileNameTemplate, List<String> libList, boolean isIgnorePrivileges)
            throws IOException, InterruptedException, URISyntaxException {
        var settings = new CoreSettings();
        PgDatabaseProvider databaseProvider = new PgDatabaseProvider();
        settings.setIgnorePrivileges(isIgnorePrivileges);
        List<String> libs = new ArrayList<>();
        for (String lib : libList) {
            libs.add(TestUtils.getPathToResource(lib, getClass()).toString());
        }
        PgDatabase dbOld = new PgDatabase();
        PgDatabase dbNew = (PgDatabase) IntegrationTestUtils.loadTestDump(databaseProvider, fileNameTemplate + FILES_POSTFIX.NEW_SQL, getClass(), settings);
        PgLibraryLoader loader = new PgLibraryLoader(dbNew, null, new HashSet<>(), settings, new NullMonitor());

        loader.loadLibraries(isIgnorePrivileges, libs);

        String script = PgCodeKeeperApi.diff(settings, dbOld, dbNew);

        IntegrationTestUtils.assertResult(script, fileNameTemplate, getClass());
    }
}
