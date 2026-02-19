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
package org.pgcodekeeper.core.it.jdbc.ch;

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
import org.pgcodekeeper.core.database.ch.ChDatabaseProvider;
import org.pgcodekeeper.core.database.ch.jdbc.ChJdbcConnector;
import org.pgcodekeeper.core.database.ch.loader.ChDumpLoader;
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

class ChJdbcLoaderTest extends JdbcLoaderTest {

    private final ChDatabaseProvider databaseProvider = new ChDatabaseProvider();

    public static final String CLEAN_DB_SCRIPT = "clean db script";

    @ParameterizedTest
    @CsvSource({
            "ch_database",
            "ch_dump_test",
            "ch_mat_view",
            "ch_table",
            "ch_view"
    })
    void chJdbcLoaderTest(String fileName) throws Exception {
        var settings = new CoreSettings();
        settings.setAddTransaction(true);
        settings.setEnableFunctionBodiesDependencies(true);
        var diffSettings = new DiffSettings(settings);

        String dumpFileName = fileName + FILES_POSTFIX.SQL;
        var path = TestUtils.getFilePath(dumpFileName, getClass());
        var dumpDb = databaseProvider.getDumpLoader(path, diffSettings).loadAndAnalyze();
        var script = Files.readString(TestUtils.getFilePath(dumpFileName, getClass()));

        var loader = createDumpLoader(() -> new ByteArrayInputStream(script.getBytes(StandardCharsets.UTF_8)),
                dumpFileName, settings, databaseProvider);
        ScriptParser parser = new ScriptParser(loader, dumpFileName, script);

        var url = TestContainerType.CH_24.getUrl();
        var startConfDb = databaseProvider.getJdbcLoader(url, diffSettings).loadAndAnalyze();
        IJdbcConnector connector = new ChJdbcConnector(url);
        try {
            new JdbcRunner(new NullMonitor()).runBatches(connector, parser.batch(), null);

            var remoteDb = databaseProvider.getJdbcLoader(url, diffSettings).loadAndAnalyze();
            List<Path> ignoreLists = List.of(TestUtils.getFilePath("ch.pgcodekeeperignore", getClass()));
            for (var ignorePath : ignoreLists) {
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
        return new ChDumpLoader(input, inputObjectName, new DiffSettings(settings));
    }
}
