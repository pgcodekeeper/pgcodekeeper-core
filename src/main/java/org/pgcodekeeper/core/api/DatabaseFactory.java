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
package org.pgcodekeeper.core.api;

import org.pgcodekeeper.core.PgCodekeeperException;
import org.pgcodekeeper.core.loader.DatabaseLoader;
import org.pgcodekeeper.core.loader.LoaderFactory;
import org.pgcodekeeper.core.loader.PgDumpLoader;
import org.pgcodekeeper.core.loader.ProjectLoader;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.model.difftree.IIgnoreList;
import org.pgcodekeeper.core.model.difftree.IgnoreSchemaList;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.monitor.NullMonitor;
import org.pgcodekeeper.core.schema.AbstractDatabase;
import org.pgcodekeeper.core.settings.ISettings;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.stream.Collectors;

/**
 * Factory class for loading database schemas from various sources.
 * <p>
 * This class provides methods to create {@link AbstractDatabase} instances
 * from different sources: JDBC connections, project directories, and database dumps.
 * Some loading method supports optional schema filtering through ignore lists.
 * </p>
 */
public final class DatabaseFactory {

    private final ISettings settings;
    private final boolean ignoreErrors;
    private final boolean needAnalyze;
    private final IMonitor monitor;

    /**
     * Constructor with default arguments
     *
     * @param settings configuration settings
     */
    public DatabaseFactory(ISettings settings) {
        this(settings, false, true);
    }

    /**
     * Constructor with default progress monitor
     *
     * @param settings     configuration settings
     * @param ignoreErrors behavior on parsing errors
     * @param needAnalyze  if true, then after loading all database objects, an analysis will be run to find dependencies
     */
    public DatabaseFactory(ISettings settings, boolean ignoreErrors, boolean needAnalyze) {
        this(settings, ignoreErrors, needAnalyze, new NullMonitor());
    }

    /**
     * @param settings     configuration settings
     * @param ignoreErrors behavior on parsing errors
     * @param needAnalyze  if true, then after loading all database objects, an analysis will be run to find dependencies
     * @param monitor      progress monitor for tracking the operation
     */
    public DatabaseFactory(ISettings settings, boolean ignoreErrors, boolean needAnalyze, IMonitor monitor) {
        this.settings = settings;
        this.ignoreErrors = ignoreErrors;
        this.needAnalyze = needAnalyze;
        this.monitor = monitor;
    }

    /**
     * Loads database from a JDBC connection.
     *
     * @param url the JDBC connection URL
     * @return the loaded database
     * @throws IOException           if I/O operations fail
     * @throws PgCodekeeperException if parsing errors occur
     * @throws InterruptedException  if operation is cancelled
     */
    public AbstractDatabase loadFromJdbc(String url)
            throws IOException, PgCodekeeperException, InterruptedException {
        return loadFromJdbc(url, null);
    }

    /**
     * Loads database from a JDBC connection with schema filtering and monitoring.
     *
     * @param url                  the JDBC connection URL
     * @param ignoreSchemaListPath path to file containing schemas to ignore, or null for no filtering
     * @return the loaded database
     * @throws IOException           if I/O operations fail
     * @throws PgCodekeeperException if parsing errors occur
     * @throws InterruptedException  if operation is cancelled
     */
    public AbstractDatabase loadFromJdbc(String url, String ignoreSchemaListPath)
            throws IOException, PgCodekeeperException, InterruptedException {
        var ignoreSchemaList = IIgnoreList.parseIgnoreList(ignoreSchemaListPath, new IgnoreSchemaList());
        var loader = LoaderFactory.createJdbcLoader(settings, url, ignoreSchemaList, monitor);
        return loadDatabaseFromLoader(loader);
    }

    /**
     * Loads database from a project directory with schema filtering.
     *
     * @param projectPath path to the project directory containing SQL files
     * @return the loaded database
     * @throws PgCodekeeperException if parsing errors occur
     * @throws IOException           if I/O operations fail
     * @throws InterruptedException  if operation is cancelled
     */
    public AbstractDatabase loadFromProject(String projectPath)
            throws PgCodekeeperException, IOException, InterruptedException {
        return loadFromProject(projectPath, null);
    }

    /**
     * Loads database from a project directory with schema filtering and monitoring.
     *
     * @param projectPath          path to the project directory containing SQL files
     * @param ignoreSchemaListPath path to file containing schemas to ignore, or null for no filtering
     * @return the loaded database
     * @throws PgCodekeeperException if parsing errors occur
     * @throws IOException           if I/O operations fail
     * @throws InterruptedException  if operation is cancelled
     */
    public AbstractDatabase loadFromProject(String projectPath, String ignoreSchemaListPath)
            throws PgCodekeeperException, IOException, InterruptedException {
        var ignoreSchemaList = IIgnoreList.parseIgnoreList(ignoreSchemaListPath, new IgnoreSchemaList());
        var loader = new ProjectLoader(projectPath, settings, monitor, new ArrayList<>(), ignoreSchemaList);
        return loadDatabaseFromLoader(loader);
    }

    /**
     * Loads database from a database dump file and monitoring.
     *
     * @param dumpPath path to the database dump file
     * @return the loaded database
     * @throws IOException           if I/O operations fail
     * @throws PgCodekeeperException if parsing errors occur
     * @throws InterruptedException  if operation is cancelled
     */
    public AbstractDatabase loadFromDump(String dumpPath)
            throws IOException, PgCodekeeperException, InterruptedException {
        var loader = new PgDumpLoader(Path.of(dumpPath), settings);
        return loadDatabaseFromLoader(loader);
    }

    private AbstractDatabase loadDatabaseFromLoader(DatabaseLoader loader) throws IOException,
            PgCodekeeperException, InterruptedException {
        AbstractDatabase db = needAnalyze ? loader.loadAndAnalyze() : loader.load();

        if (!ignoreErrors && !loader.getErrors().isEmpty()) {
            var errors = loader.getErrors().stream()
                    .map(Object::toString)
                    .collect(Collectors.joining());

            throw new PgCodekeeperException(Messages.DatabaseFactory_errors_found_while_parsing.formatted(errors));
        }

        return db;
    }
}
