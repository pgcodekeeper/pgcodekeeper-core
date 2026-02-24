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
package org.pgcodekeeper.core.database.pg;

import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.database.api.IDatabaseProvider;
import org.pgcodekeeper.core.database.api.jdbc.IJdbcConnector;
import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.api.script.IScriptBuilder;
import org.pgcodekeeper.core.database.pg.jdbc.PgJdbcConnector;
import org.pgcodekeeper.core.database.pg.loader.PgDumpLoader;
import org.pgcodekeeper.core.database.pg.loader.PgJdbcLoader;
import org.pgcodekeeper.core.database.pg.loader.PgProjectLoader;
import org.pgcodekeeper.core.database.pg.project.PgModelExporter;
import org.pgcodekeeper.core.database.pg.project.PgProjectUpdater;
import org.pgcodekeeper.core.database.pg.script.PgScriptBuilder;
import org.pgcodekeeper.core.model.difftree.TreeElement;
import org.pgcodekeeper.core.settings.DiffSettings;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.utils.InputStreamProvider;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

/**
 * {@link IDatabaseProvider} implementation for PostgreSQL databases.
 */
public class PgDatabaseProvider implements IDatabaseProvider {

    @Override
    public String getName() {
        return "PG";
    }

    @Override
    public String getFullName() {
        return "PostgreSQL";
    }

    @Override
    public IJdbcConnector getJdbcConnector(String url) {
        return new PgJdbcConnector(url);
    }

    @Override
    public PgModelExporter getModelExporter(Path outDir, IDatabase newDb, List<TreeElement> changedObjects,
                                            ISettings settings) {
        return new PgModelExporter(outDir, newDb, null, changedObjects, Consts.UTF_8, settings);
    }

    @Override
    public PgProjectUpdater getProjectUpdater(IDatabase newDb, IDatabase oldDb, List<TreeElement> changedObjects,
                                              Path projectPath, ISettings settings) {
        return new PgProjectUpdater(newDb, oldDb, changedObjects, Consts.UTF_8, projectPath, false, settings);
    }

    @Override
    public PgJdbcLoader getJdbcLoader(String url, DiffSettings diffSettings) {
        String timezone = diffSettings.getSettings().getTimeZone() == null
                ? Consts.UTC : diffSettings.getSettings().getTimeZone();
        return new PgJdbcLoader(getJdbcConnector(url), timezone, diffSettings);
    }

    @Override
    public PgDumpLoader getDumpLoader(Path path, DiffSettings diffSettings) {
        return new PgDumpLoader(path, diffSettings);
    }

    @Override
    public PgDumpLoader getDumpLoader(InputStreamProvider input, String name, DiffSettings diffSettings) {
        return new PgDumpLoader(input, name, diffSettings);
    }

    @Override
    public PgProjectLoader getProjectLoader(Path path, DiffSettings diffSettings) {
        return new PgProjectLoader(path, diffSettings);
    }

    @Override
    public PgProjectLoader getProjectLoader(Path path, DiffSettings diffSettings, Collection<String> libXmls,
                                            Collection<String> libs, Collection<String> libsWithoutPriv, Path metaPath) {
        return new PgProjectLoader(path, diffSettings, libXmls, libs, libsWithoutPriv, metaPath);
    }

    @Override
    public IScriptBuilder getScriptBuilder(DiffSettings diffSettings) {
        return new PgScriptBuilder(diffSettings);
    }
}
