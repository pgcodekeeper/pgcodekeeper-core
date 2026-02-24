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
package org.pgcodekeeper.core.database.ms;

import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.database.api.IDatabaseProvider;
import org.pgcodekeeper.core.database.api.jdbc.IJdbcConnector;
import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.api.script.IScriptBuilder;
import org.pgcodekeeper.core.database.ms.jdbc.MsJdbcConnector;
import org.pgcodekeeper.core.database.ms.loader.MsDumpLoader;
import org.pgcodekeeper.core.database.ms.loader.MsJdbcLoader;
import org.pgcodekeeper.core.database.ms.loader.MsProjectLoader;
import org.pgcodekeeper.core.database.ms.project.MsModelExporter;
import org.pgcodekeeper.core.database.ms.project.MsProjectUpdater;
import org.pgcodekeeper.core.database.ms.script.MsScriptBuilder;
import org.pgcodekeeper.core.model.difftree.TreeElement;
import org.pgcodekeeper.core.settings.DiffSettings;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.utils.InputStreamProvider;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

/**
 * {@link IDatabaseProvider} implementation for MS SQL Server databases.
 */
public class MsDatabaseProvider implements IDatabaseProvider {

    @Override
    public String getName() {
        return "MS";
    }

    @Override
    public String getFullName() {
        return "MS SQL";
    }

    @Override
    public IJdbcConnector getJdbcConnector(String url) {
        return new MsJdbcConnector(url);
    }

    @Override
    public MsModelExporter getModelExporter(Path outDir, IDatabase newDb, List<TreeElement> changedObjects,
                                            ISettings settings) {
        return new MsModelExporter(outDir, newDb, null, changedObjects, Consts.UTF_8, settings);
    }

    @Override
    public MsProjectUpdater getProjectUpdater(IDatabase newDb, IDatabase oldDb, List<TreeElement> changedObjects,
                                              Path projectPath, ISettings settings) {
        return new MsProjectUpdater(newDb, oldDb, changedObjects, Consts.UTF_8, projectPath, false, settings);
    }

    @Override
    public MsJdbcLoader getJdbcLoader(String url, DiffSettings diffSettings) {
        return new MsJdbcLoader(getJdbcConnector(url), diffSettings);
    }

    @Override
    public MsDumpLoader getDumpLoader(Path path, DiffSettings diffSettings) {
        return new MsDumpLoader(path, diffSettings);
    }

    @Override
    public MsDumpLoader getDumpLoader(InputStreamProvider input, String name, DiffSettings diffSettings) {
        return new MsDumpLoader(input, name, diffSettings);
    }

    @Override
    public MsProjectLoader getProjectLoader(Path path, DiffSettings diffSettings) {
        return new MsProjectLoader(path, diffSettings);
    }

    @Override
    public MsProjectLoader getProjectLoader(Path path, DiffSettings diffSettings, Collection<String> libXmls,
                                            Collection<String> libs, Collection<String> libsWithoutPriv, Path metaPath) {
        return new MsProjectLoader(path, diffSettings, libXmls, libs, libsWithoutPriv, metaPath);
    }

    @Override
    public IScriptBuilder getScriptBuilder(DiffSettings diffSettings) {
        return new MsScriptBuilder(diffSettings);
    }
}
