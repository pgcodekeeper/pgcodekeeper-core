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
package org.pgcodekeeper.core.database.pg.loader;

import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.database.api.jdbc.IJdbcConnector;
import org.pgcodekeeper.core.database.base.loader.AbstractLibraryLoader;
import org.pgcodekeeper.core.database.pg.jdbc.PgJdbcConnector;
import org.pgcodekeeper.core.database.pg.schema.PgDatabase;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.monitor.NullMonitor;
import org.pgcodekeeper.core.settings.ISettings;

import java.nio.file.Path;
import java.util.Set;

public class PgLibraryLoader extends AbstractLibraryLoader<PgDatabase> {

    public PgLibraryLoader(PgDatabase database, Path metaPath, Set<String> loadedPaths,
                           ISettings settings, IMonitor monitor) {
        super(database, metaPath, loadedPaths, settings, monitor);
    }

    @Override
    protected PgDatabase createDatabase() {
        return new PgDatabase();
    }

    @Override
    protected PgDumpLoader getDumpLoader(Path p, ISettings settings) {
        return new PgDumpLoader(p, settings);
    }

    @Override
    protected PgJdbcLoader createJdbcLoader(String url, ISettings settings) {
        // FIXME IgnoreSchemaList?
        IJdbcConnector con = new PgJdbcConnector(url);
        return new PgJdbcLoader(con, Consts.UTC, settings, monitor, null);
    }

    @Override
    protected PgProjectLoader getProjectLoader(Path p, ISettings copySettings) {
        return new PgProjectLoader(p, copySettings, new NullMonitor(), null);
    }

    @Override
    protected PgLibraryLoader getCopy(PgDatabase db) {
        return new PgLibraryLoader(db, metaPath, loadedPaths, settings, monitor);
    }
}
