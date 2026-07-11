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

import java.nio.file.Path;
import java.util.Collection;

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
import org.pgcodekeeper.core.database.pg.schema.PgDatabase;
import org.pgcodekeeper.core.database.pg.script.PgScriptBuilder;
import org.pgcodekeeper.core.model.difftree.TreeElement;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.utils.InputStreamProvider;

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
    public PgDatabase createDatabase() {
        return new PgDatabase();
    }

    @Override
    public IJdbcConnector getJdbcConnector(String url) {
        return new PgJdbcConnector(url);
    }

    @Override
    public PgModelExporter getModelExporter(Path outDir, IDatabase newDb, Collection<TreeElement> changedObjects,
                                            ISettings settings) {
        return new PgModelExporter(outDir, newDb, null, changedObjects, Consts.UTF_8, settings);
    }

    @Override
    public PgModelExporter getModelExporter(Path outDir, IDatabase newDb, Collection<TreeElement> changedObjects,
                                            ISettings settings, Path structureFile) {
        return new PgModelExporter(outDir, newDb, null, structureFile, changedObjects, Consts.UTF_8, settings);
    }

    @Override
    public PgProjectUpdater getProjectUpdater(IDatabase newDb, IDatabase oldDb, Collection<TreeElement> changedObjects,
                                              Path projectPath, boolean overridesOnly, ISettings settings) {
        return new PgProjectUpdater(newDb, oldDb, changedObjects, Consts.UTF_8, projectPath, overridesOnly, settings);
    }

    @Override
    public PgJdbcLoader getJdbcLoader(String url, ISettings settings) {
        return getJdbcLoader(getJdbcConnector(url), settings);
    }

    @Override
    public PgJdbcLoader getJdbcLoader(IJdbcConnector connector, ISettings settings) {
        String timezone = settings.getTimeZone() == null
                ? Consts.UTC : settings.getTimeZone();
        return new PgJdbcLoader(connector, timezone, settings);
    }

    @Override
    public PgDumpLoader getDumpLoader(Path path, ISettings settings) {
        return new PgDumpLoader(path, settings);
    }

    @Override
    public PgDumpLoader getDumpLoader(InputStreamProvider input, String name, ISettings settings) {
        return new PgDumpLoader(input, name, settings);
    }

    @Override
    public PgProjectLoader getProjectLoader(Path path, ISettings settings) {
        return new PgProjectLoader(path, settings);
    }

    @Override
    public PgProjectLoader getProjectLoader(Path path, ISettings settings, Collection<String> libXmls,
                                            Collection<String> libs, Collection<String> libsWithoutPriv, Path metaPath) {
        return new PgProjectLoader(path, settings, libXmls, libs, libsWithoutPriv, metaPath);
    }

    @Override
    public IScriptBuilder getScriptBuilder(ISettings settings) {
        return new PgScriptBuilder(settings);
    }
}
