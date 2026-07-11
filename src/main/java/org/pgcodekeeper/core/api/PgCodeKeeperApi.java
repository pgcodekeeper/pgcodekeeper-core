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
package org.pgcodekeeper.core.api;

import org.pgcodekeeper.core.DangerStatement;
import org.pgcodekeeper.core.database.api.IDatabaseProvider;
import org.pgcodekeeper.core.database.api.loader.ILoader;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.base.jdbc.JdbcRunner;
import org.pgcodekeeper.core.database.base.parser.ScriptParser;
import org.pgcodekeeper.core.ignorelist.IgnoreList;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.model.difftree.DiffTree;
import org.pgcodekeeper.core.model.difftree.TreeElement;
import org.pgcodekeeper.core.model.difftree.TreeFlattener;
import org.pgcodekeeper.core.model.graph.DepcyFinder;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.utils.Utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Main API class for pgCodeKeeper database operations.
 */
public final class PgCodeKeeperApi {

    /**
     * Compares two databases and generates a tree.
     *
     * @param oldDbLoader  loader for the old database version to compare from
     * @param newDbLoader  loader for the new database version to compare to
     * @param settings configuration settings
     * @return the root element of generated tree
     * @throws IOException          if I/O operations fail
     * @throws InterruptedException if the thread is interrupted during the operation
     */
    public static TreeElement createTree(ILoader oldDbLoader,
                                         ILoader newDbLoader,
                                         ISettings settings)
            throws IOException, InterruptedException {
        var subMonitor = settings.getMonitor().createSubMonitor();
        subMonitor.setWorkRemaining(65);

        var databases = Utils.loadDatabases(oldDbLoader, newDbLoader, settings, subMonitor);

        subMonitor.setTaskName(Messages.PgCodeKeeperApi_creating_tree);
        TreeElement root = DiffTree.create(settings, databases.getFirst(), databases.getSecond(),
                settings.getMonitor());
        subMonitor.worked(5);

        return root;
    }

    /**
     * Compares two databases and generates a migration script.
     *
     * @param provider     the database provider determining SQL dialect
     * @param oldDbLoader  loader for the old database version to compare from
     * @param newDbLoader  loader for the new database version to compare to
     * @param settings configuration settings
     * @return the generated migration script as a string
     * @throws IOException          if I/O operations fail
     * @throws InterruptedException if the thread is interrupted during the
     *                              operation
     */
    public static String diff(IDatabaseProvider provider,
                              ILoader oldDbLoader,
                              ILoader newDbLoader,
                              ISettings settings)
            throws IOException, InterruptedException {
        var subMonitor = settings.getMonitor().createSubMonitor();
        subMonitor.setWorkRemaining(70);
        var databases = Utils.loadDatabases(oldDbLoader, newDbLoader, settings, subMonitor);

        subMonitor.setTaskName(Messages.PgCodeKeeperApi_building_script);
        var script = diff(provider, databases.getFirst(), databases.getSecond(), settings);
        subMonitor.worked(10);

        return script;
    }

    /**
     * Compares two databases and generates a migration script.
     *
     * @param provider     the database provider determining SQL dialect
     * @param oldDb        the old database version to compare from
     * @param newDb        the new database version to compare to
     * @param settings configuration settings
     * @return the generated migration script as a string
     * @throws IOException          if I/O operations fail
     * @throws InterruptedException if the thread is interrupted during the operation
     */
    public static String diff(IDatabaseProvider provider,
                              IDatabase oldDb,
                              IDatabase newDb,
                              ISettings settings)
            throws IOException, InterruptedException {
        TreeElement root = DiffTree.create(settings, oldDb, newDb, settings.getMonitor());
        root.setAllChecked();
        return diff(provider, oldDb, newDb, settings, root);
    }

    /**
     * Compares two databases and generates a migration script with a pre-built tree.
     *
     * @param provider     the database provider determining SQL dialect
     * @param oldDb        the old database version to compare from
     * @param newDb        the new database version to compare to
     * @param settings configuration settings
     * @param root         root element of tree
     * @return the generated migration script as a string
     * @throws IOException if I/O operations fail
     */
    public static String diff(IDatabaseProvider provider,
                              IDatabase oldDb,
                              IDatabase newDb,
                              ISettings settings,
                              TreeElement root)
            throws IOException {
        var scriptBuilder = provider.getScriptBuilder(settings);
        return scriptBuilder.createScript(root, oldDb, newDb);
    }

    /**
     * Exports or updates project files based on database schema.
     * <p>
     * If {@code oldDb} is {@code null}, exports {@code newDb} schema to an empty project directory.
     * If {@code oldDb} is provided, updates the existing project with changes between {@code oldDb} and {@code newDb}.
     *
     * @param provider     the database provider determining SQL dialect and exporter/updater implementation
     * @param oldDbLoader  loader for the old database version (existing project state), or {@code null} for a full export
     * @param newDbLoader  loader for the new new database version (target state)
     * @param projectPath  path to the target project directory
     * @param settings configuration settings
     * @throws IOException          if I/O operations fail, if the directory does not exist,
     *                              if the directory is not empty (export) or if path is a file
     * @throws InterruptedException if the thread is interrupted during the operation
     */
    public static void exportToProject(IDatabaseProvider provider,
                                       ILoader oldDbLoader,
                                       ILoader newDbLoader,
                                       Path projectPath,
                                       ISettings settings)
            throws IOException, InterruptedException {
        exportToProject(provider, oldDbLoader, newDbLoader, projectPath, false, null, settings);
    }

    /**
     * Exports or updates project or overrides files based on database schema.
     * <p>
     * If {@code oldDb} is {@code null}, exports {@code newDb} schema to an empty project directory.
     * If {@code oldDb} is provided, updates the existing project with changes between {@code oldDb} and {@code newDb}.
     *
     * @param provider      the database provider determining SQL dialect and exporter/updater implementation
     * @param oldDbLoader   loader for the old database version (existing project state), or {@code null} for a full export
     * @param newDbLoader   loader for the new database version (target state)
     * @param projectPath   path to the target project directory
     * @param overridesOnly option to update only overrides
     * @param settings configuration settings
     * @throws IOException          if I/O operations fail, if the directory does not exist,
     *                              if the directory is not empty (export) or if path is a file
     * @throws InterruptedException if the thread is interrupted during the operation
     */
    public static void exportToProject(IDatabaseProvider provider,
                                       ILoader oldDbLoader,
                                       ILoader newDbLoader,
                                       Path projectPath,
                                       boolean overridesOnly,
                                       ISettings settings)
            throws IOException, InterruptedException {
        exportToProject(provider, oldDbLoader, newDbLoader, projectPath, overridesOnly, null, settings);
    }

    /**
     * Exports or updates project or overrides files based on database schema, using
     * an externally supplied directory layout.
     * <p>
     * If {@code oldDb} is {@code null}, exports {@code newDb} schema to an empty project directory.
     * If {@code oldDb} is provided, updates the existing project with changes between {@code oldDb} and {@code newDb}.
     *
     * @param provider      the database provider determining SQL dialect and exporter/updater implementation
     * @param oldDbLoader   loader for the old database version (existing project state), or {@code null} for a full export
     * @param newDbLoader   loader for the new database version (target state)
     * @param projectPath   path to the target project directory
     * @param overridesOnly option to update only overrides
     * @param structureFile path to a properties file containing directory layout overrides
     *                      to apply, or {@code null} to use the default layout. The file may
     *                      have any name. When non-{@code null}, the resolved layout is
     *                      persisted to the exported project as {@code structure.properties}
     *                      regardless of the source filename. Only used when {@code oldDbLoader}
     *                      is {@code null} (full export).
     * @param settings configuration settings
     * @throws IOException          if I/O operations fail, if the directory does not exist,
     *                              if the directory is not empty (export) or if path is a file
     * @throws InterruptedException if the thread is interrupted during the operation
     */
    public static void exportToProject(IDatabaseProvider provider,
                                       ILoader oldDbLoader,
                                       ILoader newDbLoader,
                                       Path projectPath,
                                       boolean overridesOnly,
                                       Path structureFile,
                                       ISettings settings)
            throws IOException, InterruptedException {
        var subMonitor = settings.getMonitor().createSubMonitor();
        subMonitor.setWorkRemaining(100);

        subMonitor.setTaskName(Messages.Utils_loading_old_database);
        var oldDb = oldDbLoader == null ? null : oldDbLoader.loadAndAnalyze();
        subMonitor.worked(30);

        subMonitor.setTaskName(Messages.Utils_loading_new_database);
        var newDb = newDbLoader.loadAndAnalyze();
        subMonitor.worked(30);

        IgnoreList ignoreList = settings.getIgnoreList();

        subMonitor.setTaskName(Messages.PgCodeKeeperApi_creating_tree);
        TreeElement root = DiffTree.create(settings, oldDb, newDb, settings.getMonitor());
        root.setAllChecked();
        subMonitor.worked(20);

        List<TreeElement> selected = new TreeFlattener()
                .onlySelected()
                .useIgnoreList(ignoreList)
                .onlyTypes(settings.getAllowedTypes())
                .flatten(root);

        subMonitor.setTaskName(Messages.PgCodeKeeperApi_exporting_project);
        exportToProject(provider, oldDb, newDb, selected, projectPath, overridesOnly, settings, structureFile);
        subMonitor.worked(20);
    }

    /**
     * Exports or updates project or overrides files based on selected elements.
     *
     * @param provider      the database provider determining SQL dialect and exporter/updater implementation
     * @param oldDb         the old database version (existing project state), or {@code null} for a full export
     * @param newDb         the new database version (target state)
     * @param selected      the selected elements
     * @param projectPath   path to the target project directory
     * @param overridesOnly option to update only overrides
     * @param settings      configuration settings
     * @throws IOException          if I/O operations fail, if the directory does not exist,
     *                              if the directory is not empty (export) or if path is a file
     */
    public static void exportToProject(IDatabaseProvider provider,
                                       IDatabase oldDb,
                                       IDatabase newDb,
                                       List<TreeElement> selected,
                                       Path projectPath,
                                       boolean overridesOnly,
                                       ISettings settings)
            throws IOException {
        exportToProject(provider, oldDb, newDb, selected, projectPath, overridesOnly, settings, null);
    }

    /**
     * Exports or updates project or overrides files based on selected elements, using
     * an externally supplied directory layout.
     *
     * @param provider      the database provider determining SQL dialect and exporter/updater implementation
     * @param oldDb         the old database version (existing project state), or {@code null} for a full export
     * @param newDb         the new database version (target state)
     * @param selected      the selected elements
     * @param projectPath   path to the target project directory
     * @param overridesOnly option to update only overrides
     * @param settings      configuration settings
     * @param structureFile path to a properties file containing directory layout overrides
     *                      to apply, or {@code null} to use the default layout. The file may
     *                      have any name. When non-{@code null}, the resolved layout is
     *                      persisted to the exported project as {@code structure.properties}
     *                      regardless of the source filename.
     * @throws IOException          if I/O operations fail, if the directory does not exist,
     *                              if the directory is not empty (export) or if path is a file
     */
    public static void exportToProject(IDatabaseProvider provider,
                                       IDatabase oldDb,
                                       IDatabase newDb,
                                       List<TreeElement> selected,
                                       Path projectPath,
                                       boolean overridesOnly,
                                       ISettings settings,
                                       Path structureFile)
            throws IOException {
        if (oldDb != null) {
            provider.getProjectUpdater(newDb, oldDb, selected, projectPath, overridesOnly, settings).updatePartial();
        } else {
            provider.getModelExporter(projectPath, newDb, selected, settings, structureFile).exportProject();
        }
    }

    /**
     * Analyzes database object dependencies and builds a dependency graph.
     *
     * @param loader       loader for the database to analyze
     * @param objectNames  collection of object name patterns to search for (e.g., "public.users", "*.orders")
     * @param depth        depth of dependency analysis (e.g., 10 levels)
     * @param reverse      false = direct dependencies (what this depends on), true = reverse dependencies (what depends on this)
     * @param filterTypes  types of objects to filter (TABLE, FUNCTION, VIEW), null = all types
     * @param invertFilter false = include only specified types, true = exclude specified types
     * @return list of strings with dependency information
     * @throws IOException          if I/O operations fail
     * @throws InterruptedException if the thread is interrupted during the operation
     */
    public static List<String> analyzeDependencies(ILoader loader,
                                                   Collection<String> objectNames,
                                                   int depth,
                                                   boolean reverse,
                                                   Collection<DbObjType> filterTypes,
                                                   boolean invertFilter)
            throws IOException, InterruptedException {
        var db = loader.loadAndAnalyze();
        var settings = loader.getSettings();
        return DepcyFinder.byPatterns(depth, reverse, filterTypes, invertFilter, db, objectNames,
                settings.getAdditionalDependencies());
    }

    /**
     * Checks SQL script for dangerous operations (DROP TABLE, ALTER COLUMN, etc.).
     *
     * @param provider       the database provider determining SQL dialect
     * @param name           name of the script source (used as file identifier for parsing)
     * @param sql            the SQL script to check
     * @param settings   parsing settings
     * @param allowedDangers set of allowed dangerous operations
     * @return set of detected dangerous operations
     * @throws IOException          if I/O operations fail
     * @throws InterruptedException if the thread is interrupted during the operation
     */
    public static Set<DangerStatement> checkDangerousStatements(IDatabaseProvider provider,
                                                                String name, String sql,
                                                                ISettings settings,
                                                                Collection<DangerStatement> allowedDangers)
            throws IOException, InterruptedException {
        var subMonitor = settings.getMonitor().createSubMonitor();
        subMonitor.setWorkRemaining(100);

        subMonitor.setTaskName(Messages.PgCodeKeeperApi_parsing_script);
        ScriptParser parser = new ScriptParser(provider.getDumpLoader(() -> new ByteArrayInputStream(
                sql.getBytes(StandardCharsets.UTF_8)), name, settings), name, sql);
        subMonitor.worked(50);

        subMonitor.setTaskName(Messages.PgCodeKeeperApi_checking_dangerous_statements);
        var result = parser.getDangerDdl(allowedDangers);
        subMonitor.worked(50);

        return result;
    }

    /**
     * Parses and executes SQL script against a database.
     *
     * @param provider     the database provider determining SQL dialect and JDBC connector
     * @param name         name of the script source (used as file identifier for parsing)
     * @param sql          the SQL script to execute
     * @param url          full JDBC URL of the target database
     * @param settings parsing and execution settings
     * @throws IOException          if there is an error reading the script
     * @throws InterruptedException if the thread is interrupted during the operation
     * @throws SQLException         if a database access error occurs during execution
     */
    public static void runSQL(IDatabaseProvider provider, String name, String sql, String url,
                              ISettings settings)
            throws IOException, InterruptedException, SQLException {
        var subMonitor = settings.getMonitor().createSubMonitor();
        subMonitor.setWorkRemaining(100);

        subMonitor.setTaskName(Messages.PgCodeKeeperApi_parsing_script);
        ScriptParser parser = new ScriptParser(provider.getDumpLoader(() -> new ByteArrayInputStream(
                sql.getBytes(StandardCharsets.UTF_8)), name, settings), name, sql);
        subMonitor.worked(30);

        subMonitor.setTaskName(Messages.PgCodeKeeperApi_executing_script);
        new JdbcRunner().runBatches(provider.getJdbcConnector(url), parser.batch(), null);
        subMonitor.worked(70);
    }

    private PgCodeKeeperApi() {
        // only statics
    }
}
