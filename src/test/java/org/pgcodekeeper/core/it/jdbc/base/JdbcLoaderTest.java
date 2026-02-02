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
package org.pgcodekeeper.core.it.jdbc.base;

import org.junit.jupiter.api.Assertions;
import org.pgcodekeeper.core.TestUtils;
import org.pgcodekeeper.core.api.PgCodeKeeperApi;
import org.pgcodekeeper.core.database.api.IDatabaseProvider;
import org.pgcodekeeper.core.database.api.jdbc.IJdbcConnector;
import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.base.jdbc.JdbcRunner;
import org.pgcodekeeper.core.database.base.loader.AbstractDumpLoader;
import org.pgcodekeeper.core.database.base.parser.ScriptParser;
import org.pgcodekeeper.core.database.base.schema.AbstractDatabase;
import org.pgcodekeeper.core.database.ch.ChDatabaseProvider;
import org.pgcodekeeper.core.database.ch.loader.ChDumpLoader;
import org.pgcodekeeper.core.database.ms.MsDatabaseProvider;
import org.pgcodekeeper.core.database.ms.loader.MsDumpLoader;
import org.pgcodekeeper.core.database.pg.PgDatabaseProvider;
import org.pgcodekeeper.core.database.pg.jdbc.SupportedPgVersion;
import org.pgcodekeeper.core.database.pg.loader.PgDumpLoader;
import org.pgcodekeeper.core.monitor.NullMonitor;
import org.pgcodekeeper.core.settings.CoreSettings;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.utils.InputStreamProvider;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public abstract class JdbcLoaderTest {

    public static final String CLEAN_DB_SCRIPT = "clean db script";

    protected void jdbcLoaderTest(String dumpFileName, String ignoreListName, String url, IJdbcConnector connector,
                                  CoreSettings settings, SupportedPgVersion version, Class<?> clazz, IDatabaseProvider databaseProvider)
            throws Exception {
        settings.setAddTransaction(true);
        jdbcLoaderTest(dumpFileName, ignoreListName, url, connector, settings, version, clazz, false, databaseProvider);
    }

    protected void jdbcLoaderTest(String dumpFileName, String ignoreListName, String url, IJdbcConnector connector,
                                  CoreSettings settings, SupportedPgVersion version, Class<?> clazz,
                                  boolean isMemoryOptimized, IDatabaseProvider databaseProvider)
            throws Exception {
        settings.setEnableFunctionBodiesDependencies(true);
        List<String> ignoreLists = new ArrayList<>();
        ignoreLists.add(TestUtils.getFilePath(ignoreListName, clazz));

        String pathToFile = TestUtils.getFilePath(dumpFileName, clazz);
        var path = Path.of(pathToFile);
        var dumpDb = databaseProvider.getDatabaseFromDump(path, settings, new NullMonitor());
        if (null != version && dumpDb instanceof AbstractDatabase abstractDatabase) {
            abstractDatabase.setVersion(version);
        }
        var script = Files.readString(TestUtils.getPathToResource(dumpFileName, clazz));

        var loader = createDumpLoader(() -> new ByteArrayInputStream(script.getBytes(StandardCharsets.UTF_8)),
                dumpFileName, settings, databaseProvider);
        ScriptParser parser = new ScriptParser(loader, dumpFileName, script);

        var startConfDb = databaseProvider.getDatabaseFromJdbc(url, settings, new NullMonitor(), null);
        try {
            var runner = new JdbcRunner(new NullMonitor());
            if (isMemoryOptimized) {
                runner.run(connector, script);
            } else {
                runner.runBatches(connector, parser.batch(), null);
            }
            var remoteDb = databaseProvider.getDatabaseFromJdbc(url, settings, new NullMonitor(), null);
            var actual = PgCodeKeeperApi.diff(settings, dumpDb, remoteDb, ignoreLists);
            Assertions.assertEquals("", actual, "Incorrect run dump %s on Database".formatted(dumpFileName));
        } finally {
            clearDb(settings, startConfDb, connector, url, databaseProvider);
        }
    }

    private void clearDb(CoreSettings settings, IDatabase startConfDb,
                         IJdbcConnector connector, String url, IDatabaseProvider databaseProvider)
            throws IOException, InterruptedException, SQLException {
        var oldDb = databaseProvider.getDatabaseFromJdbc(url, settings, new NullMonitor(), null);
        var dropScript = PgCodeKeeperApi.diff(settings, oldDb, startConfDb);

        var loader = createDumpLoader(() -> new ByteArrayInputStream(dropScript.getBytes(StandardCharsets.UTF_8)),
                CLEAN_DB_SCRIPT, settings, databaseProvider);
        new JdbcRunner(new NullMonitor()).runBatches(connector,
                new ScriptParser(loader, CLEAN_DB_SCRIPT, dropScript).batch(), null);

        var newDb = databaseProvider.getDatabaseFromJdbc(url, settings, new NullMonitor(), null);
        var diff = PgCodeKeeperApi.diff(settings, startConfDb, newDb);
        if (!diff.isEmpty()) {
            throw new IllegalStateException("Database cleared incorrect. in database:\n\n" + diff);
        }
    }

    public static AbstractDumpLoader<?> createDumpLoader(InputStreamProvider input, String inputObjectName,
                                                         ISettings settings, IDatabaseProvider databaseProvider) {
        if (databaseProvider instanceof PgDatabaseProvider) {
            return new PgDumpLoader(input, inputObjectName, settings);
        } else if (databaseProvider instanceof MsDatabaseProvider) {
            return new MsDumpLoader(input, inputObjectName, settings);
        } else if (databaseProvider instanceof ChDatabaseProvider) {
            return new ChDumpLoader(input, inputObjectName, settings);
        } else {
            throw new IllegalStateException("Unknown database type");
        }
    }

}
