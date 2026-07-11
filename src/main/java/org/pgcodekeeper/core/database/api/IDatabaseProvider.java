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

import java.nio.file.Path;
import java.util.Collection;

import org.pgcodekeeper.core.database.api.jdbc.IJdbcConnector;
import org.pgcodekeeper.core.database.api.loader.IDumpLoader;
import org.pgcodekeeper.core.database.api.loader.IJdbcLoader;
import org.pgcodekeeper.core.database.api.loader.IProjectLoader;
import org.pgcodekeeper.core.database.api.project.IModelExporter;
import org.pgcodekeeper.core.database.api.project.IProjectUpdater;
import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.api.script.IScriptBuilder;
import org.pgcodekeeper.core.model.difftree.TreeElement;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.utils.InputStreamProvider;

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
     * @return new empty database instance for this DBMS
     */
    IDatabase createDatabase();

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
    IModelExporter getModelExporter(Path outDir, IDatabase newDb, Collection<TreeElement> changedObjects,
                                    ISettings settings);

    /**
     * @param outDir         target directory for the exported project
     * @param newDb          the database schema to export
     * @param changedObjects list of changed tree elements to include in export
     * @param settings       configuration settings
     * @param structureFile  path to a properties file containing directory layout overrides
     *                       to apply, or {@code null} to use the default layout. The file
     *                       may have any name. When non-{@code null}, the resolved layout
     *                       is persisted to the exported project as {@code structure.properties}
     *                       regardless of the source filename.
     * @return model exporter for the DBMS
     */
    IModelExporter getModelExporter(Path outDir, IDatabase newDb, Collection<TreeElement> changedObjects,
                                    ISettings settings, Path structureFile);

    /**
     * @param newDb          the new database version with changes
     * @param oldDb          the old database version
     * @param changedObjects list of changed tree elements to apply
     * @param projectPath    path to the project directory to update
     * @param overridesOnly  update overrides only
     * @param settings       configuration settings
     * @return project updater for the DBMS
     */
    IProjectUpdater getProjectUpdater(IDatabase newDb, IDatabase oldDb, Collection<TreeElement> changedObjects,
                                      Path projectPath, boolean overridesOnly, ISettings settings);

    /**
     * @param url          full jdbc url
     * @param settings configuration settings
     * @return jdbc loader for the DBMS
     * @see IJdbcLoader
     * @see ISettings
     */
    IJdbcLoader getJdbcLoader(String url, ISettings settings);

    /**
     * @param connector    jdbc connector for the DBMS
     * @param settings configuration settings
     * @return jdbc loader for the DBMS
     * @see IJdbcLoader
     * @see ISettings
     */
    IJdbcLoader getJdbcLoader(IJdbcConnector connector, ISettings settings);

    /**
     * @param path         path to dump file
     * @param settings configuration settings
     * @return dump loader for the DBMS
     * @see IDumpLoader
     * @see ISettings
     */
    IDumpLoader getDumpLoader(Path path, ISettings settings);

    /**
     * @param input        input stream provider for SQL content
     * @param name         name of the source (for error reporting)
     * @param settings configuration settings
     * @return dump loader for DBMS
     */
    IDumpLoader getDumpLoader(InputStreamProvider input, String name, ISettings settings);

    /**
     * @param path         path to project directory
     * @param settings configuration settings
     * @return project loader for the DBMS
     * @see IProjectLoader
     * @see ISettings
     */
    IProjectLoader getProjectLoader(Path path, ISettings settings);

    /**
     * @param path            path to project directory
     * @param settings    configuration settings
     * @param libXmls         paths to XML files with library dependency definitions
     * @param libs            paths to library dependencies
     * @param libsWithoutPriv paths to library dependencies with ignored privileges
     * @param metaPath        path to metadata directory for storing downloaded and unzipped library files, may be null
     *                        if no ZIP or URI libraries are expected
     * @return project loader for the DBMS
     * @see IProjectLoader
     * @see ISettings
     */
    IProjectLoader getProjectLoader(Path path, ISettings settings, Collection<String> libXmls,
                                    Collection<String> libs, Collection<String> libsWithoutPriv, Path metaPath);

    /**
     * @param settings configuration settings
     * @return return script builder
     */
    IScriptBuilder getScriptBuilder(ISettings settings);
}
