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
package org.pgcodekeeper.core.it.jdbc.ms;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.pgcodekeeper.core.FILES_POSTFIX;
import org.pgcodekeeper.core.TestUtils;
import org.pgcodekeeper.core.api.PgCodeKeeperApi;
import org.pgcodekeeper.core.database.api.IDatabaseProvider;
import org.pgcodekeeper.core.database.api.jdbc.IJdbcConnector;
import org.pgcodekeeper.core.database.base.jdbc.JdbcRunner;
import org.pgcodekeeper.core.database.base.loader.AbstractDumpLoader;
import org.pgcodekeeper.core.database.base.parser.ScriptParser;
import org.pgcodekeeper.core.database.ms.MsDatabaseProvider;
import org.pgcodekeeper.core.database.ms.jdbc.MsJdbcConnector;
import org.pgcodekeeper.core.database.ms.loader.MsDumpLoader;
import org.pgcodekeeper.core.database.ms.schema.MsDatabase;
import org.pgcodekeeper.core.it.jdbc.base.JdbcLoaderTest;
import org.pgcodekeeper.core.monitor.NullMonitor;
import org.pgcodekeeper.core.settings.CoreSettings;
import org.pgcodekeeper.core.settings.DiffSettings;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.utils.InputStreamProvider;
import org.pgcodekeeper.core.utils.testcontainer.TestContainerType;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

class MsJdbcLoaderTest extends JdbcLoaderTest {

    public static final String CLEAN_DB_SCRIPT = "clean db script";

    private final MsDatabaseProvider databaseProvider = new MsDatabaseProvider();

    @ParameterizedTest
    @CsvSource({
            "ms_dump_test",
            "ms_index",
            "ms_sequence",
            "ms_statistics",
            "ms_sys_ver_table",
            "ms_table_tracking",
            "ms_table_type",
            "ms_table_with_partition",
            "ms_trigger",
            "ms_view",
    })
    void msJdbcLoaderTest(String fileName) throws Exception {
        var settings = new CoreSettings();
        settings.setAddTransaction(true);
        var url = TestContainerType.MS_16.getMsUrl(false);
        jdbcLoaderTest(fileName + FILES_POSTFIX.SQL, url, settings, false);
    }

    @ParameterizedTest
    @CsvSource({"ms_table_type",})
    void msJdbcLoaderWithMemomyOptimizedTest(String fileName) throws Exception {
        var settings = new CoreSettings();
        var url = TestContainerType.MS_16.getMsUrl(true);
        jdbcLoaderTest(fileName + "_memory_optimized" + FILES_POSTFIX.SQL, url, settings, true);
    }

    private void jdbcLoaderTest(String dumpFileName, String url, CoreSettings settings, boolean isMemoryOptimized)
            throws Exception {
        settings.setEnableFunctionBodiesDependencies(true);
        var diffSettings = new DiffSettings(settings);

        var path = TestUtils.getFilePath(dumpFileName, getClass());
        MsDatabase dumpDb = databaseProvider.getDumpLoader(path, diffSettings).loadAndAnalyze();

        var script = Files.readString(TestUtils.getFilePath(dumpFileName, getClass()));
        var loader = createDumpLoader(() -> new ByteArrayInputStream(script.getBytes(StandardCharsets.UTF_8)),
                dumpFileName, settings, databaseProvider);
        ScriptParser parser = new ScriptParser(loader, dumpFileName, script);

        var startConfDb = databaseProvider.getJdbcLoader(url, diffSettings).loadAndAnalyze();
        IJdbcConnector connector = new MsJdbcConnector(url);
        try {
            var runner = new JdbcRunner(new NullMonitor());
            if (isMemoryOptimized) {
                runner.run(connector, script);
            } else {
                runner.runBatches(connector, parser.batch(), null);
            }

            var remoteDb = databaseProvider.getJdbcLoader(url, diffSettings).loadAndAnalyze();
            List<Path> ignoreLists = List.of(TestUtils.getFilePath("ms.pgcodekeeperignore", getClass()));
            for (Path ignorePath : ignoreLists) {
                diffSettings.addIgnoreList(ignorePath);
            }
            var actual = PgCodeKeeperApi.diff(databaseProvider, dumpDb, remoteDb, diffSettings);
            Assertions.assertEquals("", actual, "Incorrect run dump %s on Database".formatted(dumpFileName));
        } finally {
            clearDb(settings, startConfDb, connector, url, databaseProvider);
        }
    }

    @Override
    protected AbstractDumpLoader<?> createDumpLoader(InputStreamProvider input, String inputObjectName,
                                                     ISettings settings, IDatabaseProvider databaseProvider) {
        return new MsDumpLoader(input, inputObjectName, new DiffSettings(settings));
    }
}
