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
import org.pgcodekeeper.core.settings.CoreSettings;
import org.pgcodekeeper.core.settings.DiffSettings;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.utils.InputStreamProvider;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

public abstract class JdbcLoaderTest {

    public static final String CLEAN_DB_SCRIPT = "clean db script";

    protected void clearDb(CoreSettings settings, IDatabase startConfDb,
                           IJdbcConnector connector, String url, IDatabaseProvider databaseProvider)
            throws IOException, InterruptedException, SQLException {
        var diffSettings = new DiffSettings(settings);
        var oldDb = databaseProvider.getJdbcLoader(url, diffSettings).loadAndAnalyze();
        var dropScript = PgCodeKeeperApi.diff(databaseProvider, oldDb, startConfDb, diffSettings);

        var loader = createDumpLoader(() -> new ByteArrayInputStream(dropScript.getBytes(StandardCharsets.UTF_8)),
                CLEAN_DB_SCRIPT, settings, databaseProvider);
        new JdbcRunner(new NullMonitor()).runBatches(connector,
                new ScriptParser(loader, CLEAN_DB_SCRIPT, dropScript).batch(), null);

        var newDb = databaseProvider.getJdbcLoader(url, diffSettings).loadAndAnalyze();
        var diff = PgCodeKeeperApi.diff(databaseProvider, startConfDb, newDb, diffSettings);
        if (!diff.isEmpty()) {
            throw new IllegalStateException("Database cleared incorrect. in database:\n\n" + diff);
        }
    }

    protected abstract IDumpLoader createDumpLoader(InputStreamProvider input, String inputObjectName,
            ISettings settings, IDatabaseProvider databaseProvider);
}
