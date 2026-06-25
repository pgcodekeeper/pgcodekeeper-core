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

import org.pgcodekeeper.core.api.PgCodeKeeperApi;
import org.pgcodekeeper.core.database.api.IDatabaseProvider;
import org.pgcodekeeper.core.database.api.jdbc.IJdbcConnector;
import org.pgcodekeeper.core.database.api.loader.IDumpLoader;
import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.base.jdbc.JdbcRunner;
import org.pgcodekeeper.core.database.base.parser.ScriptParser;
import org.pgcodekeeper.core.monitor.NullMonitor;
import org.pgcodekeeper.core.settings.DiffSettings;
import org.pgcodekeeper.core.utils.InputStreamProvider;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class JdbcLoaderTest {

    public static final String CLEAN_DB_SCRIPT = "clean db script";

    private static final Map<String, IDatabase> START_CONF_DB_CACHE = new ConcurrentHashMap<>();

    /**
     * Loads the start-configuration database from {@code url} using {@code databaseProvider}, caching it on first use
     * and returning the cached instance on subsequent calls. The cache is keyed by {@code url}.
     */
    protected IDatabase loadStartConfDb(IDatabaseProvider databaseProvider, String url, DiffSettings diffSettings)
            throws IOException, InterruptedException {
        var cached = START_CONF_DB_CACHE.get(url);
        if (cached != null) {
            return cached;
        }
        var startConfDb = databaseProvider.getJdbcLoader(url, diffSettings).loadAndAnalyze();
        START_CONF_DB_CACHE.put(url, startConfDb);
        return startConfDb;
    }

    protected void clearDb(IDatabase startConfDb, IDatabase currentDb, IJdbcConnector connector, String url,
                           IDatabaseProvider databaseProvider, DiffSettings diffSettings)
            throws IOException, InterruptedException, SQLException {
        var oldDb = currentDb != null ? currentDb
                : databaseProvider.getJdbcLoader(url, diffSettings).loadAndAnalyze();
        var dropScript = PgCodeKeeperApi.diff(databaseProvider, oldDb, startConfDb, diffSettings);

        var loader = createDumpLoader(() -> new ByteArrayInputStream(dropScript.getBytes(StandardCharsets.UTF_8)),
                CLEAN_DB_SCRIPT, diffSettings);
        new JdbcRunner(new NullMonitor()).runBatches(connector,
                new ScriptParser(loader, CLEAN_DB_SCRIPT, dropScript).batch(), null);

        var newDb = databaseProvider.getJdbcLoader(url, diffSettings).loadAndAnalyze();
        var diff = PgCodeKeeperApi.diff(databaseProvider, startConfDb, newDb, diffSettings);
        if (!diff.isEmpty()) {
            throw new IllegalStateException("Database cleared incorrect. in database:\n\n" + diff);
        }
    }

    protected abstract IDumpLoader createDumpLoader(InputStreamProvider input, String inputObjectName,
            DiffSettings diffSettings);
}
