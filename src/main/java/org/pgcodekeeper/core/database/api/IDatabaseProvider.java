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
package org.pgcodekeeper.core.database.api;

import org.pgcodekeeper.core.database.api.jdbc.IJdbcConnector;
import org.pgcodekeeper.core.database.api.loader.IDumpLoader;
import org.pgcodekeeper.core.database.api.loader.IJdbcLoader;
import org.pgcodekeeper.core.database.api.loader.IProjectLoader;
import org.pgcodekeeper.core.database.api.project.IModelExporter;
import org.pgcodekeeper.core.database.api.project.IProjectUpdater;
import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.api.script.IScriptBuilder;
import org.pgcodekeeper.core.model.difftree.TreeElement;
import org.pgcodekeeper.core.settings.DiffSettings;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.utils.InputStreamProvider;

import java.nio.file.Path;
import java.util.List;

/**
 * Interface for DBMS
 */
public interface IDatabaseProvider {

    /**
     * @return short name of DBMS
     */
    String getName();

    /**
     * @return full name of DBMS
     */
    String getFullName();

    /**
     * @param url full jdbc url
     * @return jdbc connector for DBMS
     * @see IJdbcConnector
     */
    IJdbcConnector getJdbcConnector(String url);

    /**
     * @param outDir         target directory for the exported project
     * @param newDb          the database schema to export
     * @param changedObjects list of changed tree elements to include in export
     * @param settings       configuration settings
     * @return model exporter for the DBMS
     */
    IModelExporter getModelExporter(Path outDir, IDatabase newDb, List<TreeElement> changedObjects,
                                    ISettings settings);

    /**
     * @param newDb          the new database version with changes
     * @param oldDb          the old database version
     * @param changedObjects list of changed tree elements to apply
     * @param projectPath    path to the project directory to update
     * @param settings       configuration settings
     * @return project updater for the DBMS
     */
    IProjectUpdater getProjectUpdater(IDatabase newDb, IDatabase oldDb, List<TreeElement> changedObjects,
                                      Path projectPath, ISettings settings);

    /**
     * @param url          full jdbc url
     * @param diffSettings unified context object containing settings, monitor, ignore schema list, and error accumulator
     * @return jdbc loader for the DBMS
     * @see IJdbcLoader
     * @see DiffSettings
     */
    IJdbcLoader getJdbcLoader(String url, DiffSettings diffSettings);

    /**
     * @param path         path to dump file
     * @param diffSettings unified context object containing settings, monitor, and error accumulator
     * @return dump loader for the DBMS
     * @see IDumpLoader
     * @see DiffSettings
     */
    IDumpLoader getDumpLoader(Path path, DiffSettings diffSettings);

    /**
     * @param input        input stream provider for SQL content
     * @param name         name of the source (for error reporting)
     * @param diffSettings configuration settings
     * @return dump loader for DBMS
     */
    IDumpLoader getDumpLoader(InputStreamProvider input, String name, DiffSettings diffSettings);

    /**
     * @param path         path to project directory
     * @param diffSettings unified context object containing settings, monitor, ignore schema list, and error accumulator
     * @return project loader for the DBMS
     * @see IProjectLoader
     * @see DiffSettings
     */
    IProjectLoader getProjectLoader(Path path, DiffSettings diffSettings);

    /**
     * @param diffSettings configuration settings
     * @return return script builder
     */
    IScriptBuilder getScriptBuilder(DiffSettings diffSettings);
}
