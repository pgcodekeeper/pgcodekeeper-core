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

import org.junit.jupiter.api.Assertions;
import org.pgcodekeeper.core.FILES_POSTFIX;
import org.pgcodekeeper.core.TestUtils;
import org.pgcodekeeper.core.api.PgCodeKeeperApi;
import org.pgcodekeeper.core.database.api.jdbc.IJdbcConnector;
import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.base.jdbc.JdbcRunner;
import org.pgcodekeeper.core.database.base.loader.AbstractDumpLoader;
import org.pgcodekeeper.core.database.base.parser.ScriptParser;
import org.pgcodekeeper.core.database.pg.PgDatabaseProvider;
import org.pgcodekeeper.core.database.pg.jdbc.PgJdbcConnector;
import org.pgcodekeeper.core.database.pg.loader.PgDumpLoader;
import org.pgcodekeeper.core.it.jdbc.base.JdbcLoaderTest;
import org.pgcodekeeper.core.monitor.NullMonitor;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.utils.InputStreamProvider;
import org.pgcodekeeper.core.utils.testcontainer.TestContainerType;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

abstract class AbstractPgGpJdbcLoaderTest extends JdbcLoaderTest {

    private final PgDatabaseProvider databaseProvider = new PgDatabaseProvider();

    protected void jdbcLoaderTest(boolean hasDiff, String fileName, String contTypeName, ISettings settings)
            throws Exception {
        var url = TestContainerType.valueOf(contTypeName).getUrl();
        var lowerCaseTypeName = contTypeName.toLowerCase(Locale.ROOT);
        var ignoreListName = lowerCaseTypeName + ".pgcodekeeperignore";
        var dumpFileName = lowerCaseTypeName + "_" + fileName + FILES_POSTFIX.SQL;
        var path = TestUtils.getFilePath(dumpFileName, getClass());
        var dumpDb = databaseProvider.getDumpLoader(path, settings).loadAndAnalyze();
        var script = Files.readString(TestUtils.getFilePath(dumpFileName, getClass()));

        var loader = createDumpLoader(() -> new ByteArrayInputStream(script.getBytes(StandardCharsets.UTF_8)),
                dumpFileName, settings);
        ScriptParser parser = new ScriptParser(loader, dumpFileName, script);

        var startConfDb = loadStartConfDb(databaseProvider, url, settings);
        IJdbcConnector connector = new PgJdbcConnector(url);
        IDatabase remoteDb = null;
        try {
            new JdbcRunner(new NullMonitor()).runBatches(connector, parser.batch(), null);

            remoteDb = databaseProvider.getJdbcLoader(url, settings).loadAndAnalyze();
            List<Path> ignoreLists = List.of(TestUtils.getFilePath(ignoreListName, getClass()));
            for (var ignoreList : ignoreLists) {
                settings.addIgnoreList(ignoreList);
            }
            var actual = PgCodeKeeperApi.diff(databaseProvider, dumpDb, remoteDb, settings);

            String expected;
            if (hasDiff) {
                expected = Files.readString(TestUtils.getFilePath(
                        lowerCaseTypeName + "_" + fileName + FILES_POSTFIX.DIFF_SQL, getClass()));
            } else {
                expected = "";
            }

            Assertions.assertEquals(expected, actual, "Incorrect run dump %s on Database".formatted(dumpFileName));
        } finally {
            clearDb(startConfDb, remoteDb, connector, url, databaseProvider, settings);
        }
    }

    @Override
    protected AbstractDumpLoader<?> createDumpLoader(InputStreamProvider input, String inputObjectName,
            ISettings settings) {
        return new PgDumpLoader(input, inputObjectName, settings);
    }
}
