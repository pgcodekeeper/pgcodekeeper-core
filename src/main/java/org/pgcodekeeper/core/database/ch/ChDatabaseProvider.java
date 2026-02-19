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
package org.pgcodekeeper.core.database.ch;

import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.database.api.IDatabaseProvider;
import org.pgcodekeeper.core.database.api.jdbc.IJdbcConnector;
import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.api.script.IScriptBuilder;
import org.pgcodekeeper.core.database.ch.jdbc.ChJdbcConnector;
import org.pgcodekeeper.core.database.ch.loader.ChDumpLoader;
import org.pgcodekeeper.core.database.ch.loader.ChJdbcLoader;
import org.pgcodekeeper.core.database.ch.loader.ChProjectLoader;
import org.pgcodekeeper.core.database.ch.project.ChModelExporter;
import org.pgcodekeeper.core.database.ch.project.ChProjectUpdater;
import org.pgcodekeeper.core.database.ch.script.ChScriptBuilder;
import org.pgcodekeeper.core.model.difftree.TreeElement;
import org.pgcodekeeper.core.settings.DiffSettings;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.utils.InputStreamProvider;

import java.nio.file.Path;
import java.util.List;

/**
 * {@link IDatabaseProvider} implementation for ClickHouse databases.
 */
public class ChDatabaseProvider implements IDatabaseProvider {

    @Override
    public String getName() {
        return "CH";
    }

    @Override
    public String getFullName() {
        return "ClickHouse";
    }

    @Override
    public IJdbcConnector getJdbcConnector(String url) {
        return new ChJdbcConnector(url);
    }

    @Override
    public ChModelExporter getModelExporter(Path outDir, IDatabase newDb, List<TreeElement> changedObjects,
                                            ISettings settings) {
        return new ChModelExporter(outDir, newDb, null, changedObjects, Consts.UTF_8, settings);
    }

    @Override
    public ChProjectUpdater getProjectUpdater(IDatabase newDb, IDatabase oldDb, List<TreeElement> changedObjects,
                                              Path projectPath, ISettings settings) {
        return new ChProjectUpdater(newDb, oldDb, changedObjects, Consts.UTF_8, projectPath, false, settings);
    }

    @Override
    public ChJdbcLoader getJdbcLoader(String url, DiffSettings diffSettings) {
        return new ChJdbcLoader(getJdbcConnector(url), diffSettings);
    }

    @Override
    public ChDumpLoader getDumpLoader(Path path, DiffSettings diffSettings) {
        return new ChDumpLoader(path, diffSettings);
    }

    @Override
    public ChDumpLoader getDumpLoader(InputStreamProvider input, String name, DiffSettings diffSettings) {
        return new ChDumpLoader(input, name, diffSettings);
    }

    @Override
    public ChProjectLoader getProjectLoader(Path path, DiffSettings diffSettings) {
        return new ChProjectLoader(path, diffSettings);
    }

    @Override
    public IScriptBuilder getScriptBuilder(DiffSettings diffSettings) {
        return new ChScriptBuilder(diffSettings);
    }
}
