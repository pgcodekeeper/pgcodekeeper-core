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
import org.pgcodekeeper.core.model.difftree.DiffTree;
import org.pgcodekeeper.core.model.difftree.TreeElement;
import org.pgcodekeeper.core.model.difftree.TreeFlattener;
import org.pgcodekeeper.core.model.graph.DepcyFinder;
import org.pgcodekeeper.core.settings.DiffSettings;
import org.pgcodekeeper.core.settings.ISettings;

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
     * Compares two databases and generates a migration script.
     *
     * @param provider     the database provider determining SQL dialect
     * @param oldDbLoader  loader for the old database version to compare from
     * @param newDbLoader  loader for the new database version to compare to
     * @param diffSettings unified context object containing settings, ignore list, and error accumulator
     * @return the generated migration script as a string
     * @throws IOException          if I/O operations fail
     * @throws InterruptedException if the thread is interrupted during the operation
     */
    public static String diff(IDatabaseProvider provider,
                              ILoader oldDbLoader,
                              ILoader newDbLoader,
                              DiffSettings diffSettings)
            throws IOException, InterruptedException {
        // TODO parallel load
        var oldDb = oldDbLoader.loadAndAnalyze();
        var newDb = newDbLoader.loadAndAnalyze();
        return diff(provider, oldDb, newDb, diffSettings);
    }

    /**
     * Compares two databases and generates a migration script.
     *
     * @param provider     the database provider determining SQL dialect
     * @param oldDb        the old database version to compare from
     * @param newDb        the new database version to compare to
     * @param diffSettings unified context object containing settings, ignore list, and error accumulator
     * @return the generated migration script as a string
     * @throws IOException          if I/O operations fail
     * @throws InterruptedException if the thread is interrupted during the operation
     */
    public static String diff(IDatabaseProvider provider,
                              IDatabase oldDb,
                              IDatabase newDb,
                              DiffSettings diffSettings)
            throws IOException, InterruptedException {
        TreeElement root = DiffTree.create(diffSettings.getSettings(), oldDb, newDb);
        root.setAllChecked();
        return diff(provider, oldDb, newDb, diffSettings, root);
    }

    /**
     * Compares two databases and generates a migration script with a pre-built tree.
     *
     * @param provider     the database provider determining SQL dialect
     * @param oldDb        the old database version to compare from
     * @param newDb        the new database version to compare to
     * @param diffSettings unified context object containing settings, ignore list, and error accumulator
     * @param root         root element of tree
     * @return the generated migration script as a string
     * @throws IOException if I/O operations fail
     */
    public static String diff(IDatabaseProvider provider,
                              IDatabase oldDb,
                              IDatabase newDb,
                              DiffSettings diffSettings,
                              TreeElement root)
            throws IOException {
        var scriptBuilder = provider.getScriptBuilder(diffSettings);
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
     * @param diffSettings unified context object containing settings, monitor, ignore list, and error accumulator
     * @throws IOException          if I/O operations fail, if the directory does not exist,
     *                              if the directory is not empty (export) or if path is a file
     * @throws InterruptedException if the thread is interrupted during the operation
     */
    public static void exportToProject(IDatabaseProvider provider,
                                       ILoader oldDbLoader,
                                       ILoader newDbLoader,
                                       Path projectPath,
                                       DiffSettings diffSettings)
            throws IOException, InterruptedException {
        var oldDb = oldDbLoader == null ? null : oldDbLoader.loadAndAnalyze();
        var newDb = newDbLoader.loadAndAnalyze();
        IgnoreList ignoreList = diffSettings.getIgnoreList();
        ISettings settings = diffSettings.getSettings();
        TreeElement root = DiffTree.create(settings, oldDb, newDb, diffSettings.getMonitor());
        root.setAllChecked();

        List<TreeElement> selected = new TreeFlattener()
                .onlySelected()
                .useIgnoreList(ignoreList)
                .onlyTypes(settings.getAllowedTypes())
                .flatten(root);

        if (oldDb != null) {
            provider.getProjectUpdater(newDb, oldDb, selected, projectPath, settings).updatePartial();
        } else {
            provider.getModelExporter(projectPath, newDb, selected, settings).exportProject();
        }
    }

    /**
     * Analyzes database object dependencies and builds a dependency graph.
     *
     * @param loader       loader for the the database to analyze
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
        return DepcyFinder.byPatterns(depth, reverse, filterTypes, invertFilter, db, objectNames);
    }

    /**
     * Checks SQL script for dangerous operations (DROP TABLE, ALTER COLUMN, etc.).
     *
     * @param provider       the database provider determining SQL dialect
     * @param name           name of the script source (used as file identifier for parsing)
     * @param sql            the SQL script to check
     * @param diffSettings   parsing settings
     * @param allowedDangers set of allowed dangerous operations
     * @return set of detected dangerous operations
     * @throws IOException          if I/O operations fail
     * @throws InterruptedException if the thread is interrupted during the operation
     */
    public static Set<DangerStatement> checkDangerousStatements(IDatabaseProvider provider,
                                                                String name, String sql,
                                                                DiffSettings diffSettings,
                                                                Collection<DangerStatement> allowedDangers)
            throws IOException, InterruptedException {
        ScriptParser parser = new ScriptParser(provider.getDumpLoader(() -> new ByteArrayInputStream(
                sql.getBytes(StandardCharsets.UTF_8)), name, diffSettings), name, sql);
        return parser.getDangerDdl(allowedDangers);
    }

    /**
     * Parses and executes SQL script against a database.
     *
     * @param provider     the database provider determining SQL dialect and JDBC connector
     * @param name         name of the script source (used as file identifier for parsing)
     * @param sql          the SQL script to execute
     * @param url          full JDBC URL of the target database
     * @param diffSettings parsing and execution settings
     * @throws IOException          if there is an error reading the script
     * @throws InterruptedException if the thread is interrupted during the operation
     * @throws SQLException         if a database access error occurs during execution
     */
    public static void runSQL(IDatabaseProvider provider, String name, String sql, String url,
                              DiffSettings diffSettings)
            throws IOException, InterruptedException, SQLException {
        ScriptParser parser = new ScriptParser(provider.getDumpLoader(() -> new ByteArrayInputStream(
                sql.getBytes(StandardCharsets.UTF_8)), name, diffSettings), name, sql);
        new JdbcRunner().runBatches(provider.getJdbcConnector(url), parser.batch(), null);
    }

    private PgCodeKeeperApi() {
        // only statics
    }
}
