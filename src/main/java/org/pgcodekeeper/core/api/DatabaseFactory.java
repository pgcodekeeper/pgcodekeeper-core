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
import org.pgcodekeeper.core.schema.AbstractDatabase;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.monitor.NullMonitor;

import java.io.IOException;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.stream.Collectors;

/**
 * Factory class for loading database schemas from various sources.
 * <p>
 * This class provides static methods to create {@link AbstractDatabase} instances
 * from different sources: JDBC connections, project directories, and database dumps.
 * Some loading method supports optional schema filtering through ignore lists.
 * </p>
 */
public final class DatabaseFactory {

    /**
     * Loads database from a JDBC connection.
     *
     * @param settings ISettings object
     * @param url      the JDBC connection URL
     * @return the loaded database
     * @throws IOException           if I/O operations fail
     * @throws PgCodekeeperException if database loading is interrupted or parsing errors occur
     */
    public static AbstractDatabase loadFromJdbc(ISettings settings, String url)
            throws IOException, PgCodekeeperException, InterruptedException {
        return loadFromJdbc(settings, url, null, new NullMonitor());
    }

    /**
     * Loads database from a JDBC connection with schema filtering and monitoring.
     *
     * @param settings             ISettings object
     * @param url                  the JDBC connection URL
     * @param ignoreSchemaListPath path to file containing schemas to ignore, or null for no filtering
     * @param monitor              progress monitor for tracking the operation
     * @return the loaded database
     * @throws IOException           if I/O operations fail
     * @throws PgCodekeeperException if database loading is interrupted or parsing errors occur
     */
    public static AbstractDatabase loadFromJdbc(ISettings settings, String url, String ignoreSchemaListPath,
                                                IMonitor monitor)
            throws IOException, PgCodekeeperException, InterruptedException {
        var ignoreSchemaList = IIgnoreList.parseIgnoreList(ignoreSchemaListPath, new IgnoreSchemaList());
        var loader = LoaderFactory.createJdbcLoader(settings, url, ignoreSchemaList, monitor);
        return loadDatabaseFromLoader(loader);
    }

    /**
     * Loads database from a project directory with schema filtering.
     *
     * @param settings    ISettings object
     * @param projectPath path to the project directory containing SQL files
     * @return the loaded database
     * @throws PgCodekeeperException if database loading is interrupted or parsing errors occur
     * @throws IOException           if I/O operations fail
     */
    public static AbstractDatabase loadFromProject(ISettings settings, String projectPath)
            throws PgCodekeeperException, IOException, InterruptedException {
        return loadFromProject(settings, projectPath, null, new NullMonitor());
    }

    /**
     * Loads database from a project directory with schema filtering and monitoring.
     *
     * @param settings             ISettings object
     * @param projectPath          path to the project directory containing SQL files
     * @param ignoreSchemaListPath path to file containing schemas to ignore, or null for no filtering
     * @param monitor              progress monitor for tracking the operation
     * @return the loaded database
     * @throws PgCodekeeperException if database loading is interrupted or parsing errors occur
     * @throws IOException           if I/O operations fail
     */
    public static AbstractDatabase loadFromProject(ISettings settings, String projectPath, String ignoreSchemaListPath,
                                                   IMonitor monitor)
            throws PgCodekeeperException, IOException, InterruptedException {
        var ignoreSchemaList = IIgnoreList.parseIgnoreList(ignoreSchemaListPath, new IgnoreSchemaList());
        var loader = new ProjectLoader(projectPath, settings, monitor, new ArrayList<>(), ignoreSchemaList);
        return loadDatabaseFromLoader(loader);
    }

    /**
     * Loads database from a database dump file and monitoring.
     *
     * @param settings ISettings object
     * @param dumpPath path to the database dump file
     * @param monitor  progress monitor for tracking the operation
     * @return the loaded database
     * @throws IOException           if I/O operations fail
     * @throws PgCodekeeperException if database loading is interrupted or parsing errors occur
     */
    public static AbstractDatabase loadFromDump(ISettings settings, String dumpPath, IMonitor monitor)
            throws IOException, PgCodekeeperException, InterruptedException {
        var loader = new PgDumpLoader(Path.of(dumpPath), settings, monitor);
        return loadDatabaseFromLoader(loader);
    }

    private static AbstractDatabase loadDatabaseFromLoader(DatabaseLoader loader) throws IOException,
            PgCodekeeperException, InterruptedException {
        AbstractDatabase db;
        db = loader.loadAndAnalyze();

        if (!loader.getErrors().isEmpty()) {
            var errors = loader.getErrors().stream()
                    .map(Object::toString)
                    .collect(Collectors.joining());

            throw new PgCodekeeperException(MessageFormat.format(
                    Messages.DatabaseFactory_errors_found_while_parsing, errors)
            );
        }

        return db;
    }
}
