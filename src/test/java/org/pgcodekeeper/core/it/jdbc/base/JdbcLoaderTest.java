/*******************************************************************************
 * Copyright 2017-2025 TAXTELECOM, LLC
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
import org.pgcodekeeper.core.exception.PgCodeKeeperException;
import org.pgcodekeeper.core.TestUtils;
import org.pgcodekeeper.core.api.DatabaseFactory;
import org.pgcodekeeper.core.api.PgCodeKeeperApi;
import org.pgcodekeeper.core.database.api.jdbc.IJdbcConnector;
import org.pgcodekeeper.core.database.pg.loader.jdbc.SupportedPgVersion;
import org.pgcodekeeper.core.loader.JdbcRunner;
import org.pgcodekeeper.core.monitor.NullMonitor;
import org.pgcodekeeper.core.parsers.antlr.base.ScriptParser;
import org.pgcodekeeper.core.database.base.schema.AbstractDatabase;
import org.pgcodekeeper.core.settings.CoreSettings;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public abstract class JdbcLoaderTest {

    protected void jdbcLoaderTest(String dumpFileName, String ignoreListName, String url, IJdbcConnector connector,
                                  CoreSettings settings, SupportedPgVersion version, Class<?> clazz)
            throws PgCodeKeeperException, SQLException, IOException, URISyntaxException, InterruptedException{
        settings.setAddTransaction(true);
        jdbcLoaderTest(dumpFileName, ignoreListName, url, connector, settings, version, clazz, false);
    }

    protected void jdbcLoaderTest(String dumpFileName, String ignoreListName, String url, IJdbcConnector connector,
                                  CoreSettings settings, SupportedPgVersion version, Class<?> clazz, boolean isMemoryOptimized)
            throws PgCodeKeeperException, SQLException, IOException, URISyntaxException, InterruptedException {
        settings.setEnableFunctionBodiesDependencies(true);
        var df = new DatabaseFactory(settings);
        List<String> ignoreLists = new ArrayList<>() ;
        ignoreLists.add(TestUtils.getFilePath(ignoreListName, clazz));

        var dumpDb = df.loadFromDump(TestUtils.getFilePath(dumpFileName, clazz));
        if (null != version) {
            dumpDb.setVersion(version);
        }
        var script = Files.readString(TestUtils.getPathToResource(dumpFileName, clazz));

        ScriptParser parser = new ScriptParser(dumpFileName,
                Files.readString(TestUtils.getPathToResource(dumpFileName, clazz)), settings);

        var startConfDb = df.loadFromJdbc(url);
        try {
            var runner = new JdbcRunner(new NullMonitor());
            if (isMemoryOptimized) {
                runner.run(connector, script);
            } else {
                runner.runBatches(connector, parser.batch(), null);
            }
            var remoteDb = df.loadFromJdbc(url);
            var actual = PgCodeKeeperApi.diff(settings, dumpDb, remoteDb, ignoreLists);
            Assertions.assertEquals("", actual, "Incorrect run dump %s on Database".formatted(dumpFileName));
        } finally {
            clearDb(settings, startConfDb, df, connector, url);
        }
    }

    private void clearDb(CoreSettings settings, AbstractDatabase startConfDb, DatabaseFactory df,
                               IJdbcConnector connector, String url)
            throws PgCodeKeeperException, IOException, InterruptedException, SQLException {
        var oldDb = df.loadFromJdbc(url);
        var dropScript = PgCodeKeeperApi.diff(settings, oldDb, startConfDb);
        new JdbcRunner(new NullMonitor())
                .runBatches(connector, new ScriptParser("clean db script", dropScript, settings).batch(), null);
        var diff = PgCodeKeeperApi.diff(settings, startConfDb, df.loadFromJdbc(url));
        if (!diff.isEmpty()) {
            throw new IllegalStateException("Database cleared incorrect. in database:\n\n" + diff);
        }
    }
}
