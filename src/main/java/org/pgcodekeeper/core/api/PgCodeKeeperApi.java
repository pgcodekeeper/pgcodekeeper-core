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

import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.PgDiff;
import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.database.base.project.AbstractModelExporter;
import org.pgcodekeeper.core.database.base.project.AbstractProjectUpdater;
import org.pgcodekeeper.core.database.ch.project.ChModelExporter;
import org.pgcodekeeper.core.database.ch.project.ChProjectUpdater;
import org.pgcodekeeper.core.database.ch.schema.ChDatabase;
import org.pgcodekeeper.core.database.ms.project.MsModelExporter;
import org.pgcodekeeper.core.database.ms.project.MsProjectUpdater;
import org.pgcodekeeper.core.database.ms.schema.MsDatabase;
import org.pgcodekeeper.core.database.pg.project.PgModelExporter;
import org.pgcodekeeper.core.database.pg.project.PgProjectUpdater;
import org.pgcodekeeper.core.database.pg.schema.PgDatabase;
import org.pgcodekeeper.core.ignorelist.IgnoreList;
import org.pgcodekeeper.core.ignorelist.IgnoreParser;
import org.pgcodekeeper.core.model.difftree.DiffTree;
import org.pgcodekeeper.core.model.difftree.TreeElement;
import org.pgcodekeeper.core.model.difftree.TreeFlattener;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.settings.ISettings;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Main API class for pgCodeKeeper database operations.
 */
public final class PgCodeKeeperApi {

    /**
     * Compares two databases and generates a migration script.
     *
     * @param settings ISettings object
     * @param oldDb    the old database version to compare from
     * @param newDb    the new database version to compare to
     * @return the generated migration script as a string
     * @throws IOException          if I/O operations fail
     * @throws InterruptedException if the thread is interrupted during the operation
     */
    public static String diff(ISettings settings, IDatabase oldDb, IDatabase newDb)
            throws IOException, InterruptedException {
        return diff(settings, oldDb, newDb, Collections.emptyList());
    }

    /**
     * Compares two databases and generates a migration script with filtering.
     *
     * @param settings    ISettings object
     * @param oldDb       the old database version to compare from
     * @param newDb       the new database version to compare to
     * @param ignoreLists collection of paths to files containing objects to ignore
     * @return the generated migration script as a string
     * @throws IOException          if I/O operations fail or ignore list file cannot be read
     * @throws InterruptedException if the thread is interrupted during the operation
     */
    public static String diff(ISettings settings,
                              IDatabase oldDb,
                              IDatabase newDb,
                              Collection<String> ignoreLists)
            throws IOException, InterruptedException {
        return diff(settings, oldDb, newDb, null, null, ignoreLists);
    }

    /**
     * Compares two databases and generates a migration script with filtering and additional dependencies.
     *
     * @param settings                    ISettings object
     * @param oldDb                       the old database version to compare from
     * @param newDb                       the new database version to compare to
     * @param additionalDependenciesOldDb additional dependencies in old database
     * @param additionalDependenciesNewDb additional dependencies in new database
     * @param ignoreLists                 collection of paths to files containing objects to ignore
     * @return the generated migration script as a string
     * @throws IOException          if I/O operations fail or ignore list file cannot be read
     * @throws InterruptedException if the thread is interrupted during the operation
     */
    public static String diff(ISettings settings,
                              IDatabase oldDb,
                              IDatabase newDb,
                              List<Map.Entry<IStatement, IStatement>> additionalDependenciesOldDb,
                              List<Map.Entry<IStatement, IStatement>> additionalDependenciesNewDb,
                              Collection<String> ignoreLists)
            throws IOException, InterruptedException {
        TreeElement root = DiffTree.create(settings, oldDb, newDb);
        root.setAllChecked();
        return diff(settings, root, oldDb, newDb, additionalDependenciesOldDb, additionalDependenciesNewDb, ignoreLists);
    }

    /**
     * Compares two databases and generates a migration script with filtering and additional dependencies.
     *
     * @param settings                    ISettings object
     * @param root                        root element of tree
     * @param oldDb                       the old database version to compare from
     * @param newDb                       the new database version to compare to
     * @param additionalDependenciesOldDb additional dependencies in old database
     * @param additionalDependenciesNewDb additional dependencies in new database
     * @param ignoreLists                 collection of paths to files containing objects to ignore
     * @return the generated migration script as a string
     * @throws IOException if I/O operations fail or ignore list file cannot be read
     */
    public static String diff(ISettings settings,
                              TreeElement root,
                              IDatabase oldDb,
                              IDatabase newDb,
                              List<Map.Entry<IStatement, IStatement>> additionalDependenciesOldDb,
                              List<Map.Entry<IStatement, IStatement>> additionalDependenciesNewDb,
                              Collection<String> ignoreLists)
            throws IOException {
        IgnoreList ignoreList = IgnoreParser.parseLists(ignoreLists);
        return new PgDiff(settings)
                .diff(root, oldDb, newDb, additionalDependenciesOldDb, additionalDependenciesNewDb, ignoreList);
    }

    /**
     * Exports database schema to project files.
     *
     * @param settings   ISettings object
     * @param dbToExport the database to export
     * @param exportTo   path to the target project directory
     * @throws IOException          if I/O operations fail, if export directory does not exist,
     *                              if export directory is not empty or if path is a file
     * @throws InterruptedException if the thread is interrupted during the operation
     */
    public static void export(ISettings settings, IDatabase dbToExport, String exportTo)
            throws IOException, InterruptedException {
        export(settings, dbToExport, exportTo, Collections.emptyList(), null);
    }

    /**
     * Exports database schema to project files with filtering and progress tracking.
     *
     * @param settings    ISettings object
     * @param dbToExport  the database to export
     * @param exportTo    path to the target project directory
     * @param ignoreLists collection of paths to files containing objects to ignore
     * @param monitor     progress monitor for tracking the operation
     * @throws IOException          if I/O operations fail, if export directory does not exist,
     *                              if export directory is not empty, if path is a file,
     *                              or if ignore list file cannot be read
     * @throws InterruptedException if the thread is interrupted during the operation
     */
    public static void export(ISettings settings, IDatabase dbToExport, String exportTo,
                              Collection<String> ignoreLists, IMonitor monitor)
            throws IOException, InterruptedException {
        IgnoreList ignoreList = IgnoreParser.parseLists(ignoreLists);
        TreeElement root = DiffTree.create(settings, dbToExport, null, monitor);
        root.setAllChecked();

        List<TreeElement> selected = getSelectedElements(settings, root, ignoreList);
        createModelExporter(Paths.get(exportTo), dbToExport, selected,
                settings).exportProject();
    }

    private static AbstractModelExporter createModelExporter(Path outDir, IDatabase newDb,
                                                             List<TreeElement> changedObjects,
                                                             ISettings settings) {
        if (newDb instanceof ChDatabase) {
            return new ChModelExporter(outDir, newDb, null, changedObjects, Consts.UTF_8, settings);
        } else if (newDb instanceof MsDatabase) {
            return new MsModelExporter(outDir, newDb, null, changedObjects, Consts.UTF_8, settings);
        } else if (newDb instanceof PgDatabase) {
            return new PgModelExporter(outDir, newDb, null, changedObjects, Consts.UTF_8, settings);
        }
        throw new IllegalArgumentException("Unsupported database type: " + newDb.getClass().getName());
    }

    /**
     * Updates project with changes from database.
     *
     * @param settings        ISettings object
     * @param oldDb           the old database version
     * @param newDb           the new database version with changes
     * @param projectToUpdate path to the project directory to update
     * @throws IOException          if I/O operations fail, if project directory does not exist or if path is a file
     * @throws InterruptedException if the thread is interrupted during the operation
     */
    public static void update(ISettings settings, IDatabase oldDb, IDatabase newDb, String projectToUpdate)
            throws IOException, InterruptedException {
        update(settings, oldDb, newDb, projectToUpdate, Collections.emptyList(), null);
    }

    /**
     * Updates project with changes from database with filtering and progress tracking.
     *
     * @param settings        ISettings object
     * @param oldDb           the old database version
     * @param newDb           the new database version with changes
     * @param projectToUpdate path to the project directory to update
     * @param ignoreLists     collection of paths to files containing objects to ignore
     * @param monitor         progress monitor for tracking the operation
     * @throws IOException          if I/O operations fail, if project directory does not exist, if path is a file,
     *                              or if ignore list files cannot be read
     * @throws InterruptedException if the thread is interrupted during the operation
     */
    public static void update(ISettings settings, IDatabase oldDb, IDatabase newDb,
                              String projectToUpdate, Collection<String> ignoreLists, IMonitor monitor)
            throws IOException, InterruptedException {
        IgnoreList ignoreList = IgnoreParser.parseLists(ignoreLists);
        TreeElement root = DiffTree.create(settings, oldDb, newDb, monitor);
        root.setAllChecked();
        List<TreeElement> selected = getSelectedElements(settings, root, ignoreList);

        createProjectUpdater(newDb, oldDb, selected, Paths.get(projectToUpdate), settings).updatePartial();
    }

    private static AbstractProjectUpdater createProjectUpdater(IDatabase newDb, IDatabase oldDb,
                                                               List<TreeElement> changedObjects,
                                                               Path projectPath, ISettings settings) {
        if (newDb instanceof ChDatabase) {
            return new ChProjectUpdater(newDb, oldDb, changedObjects, Consts.UTF_8, projectPath, false, settings);
        } else if (newDb instanceof MsDatabase) {
            return new MsProjectUpdater(newDb, oldDb, changedObjects, Consts.UTF_8, projectPath, false, settings);
        } else if (newDb instanceof PgDatabase) {
            return new PgProjectUpdater(newDb, oldDb, changedObjects, Consts.UTF_8, projectPath, false, settings);
        }
        throw new IllegalArgumentException("Unsupported database type: " + newDb.getClass().getName());
    }

    private static List<TreeElement> getSelectedElements(ISettings settings, TreeElement root, IgnoreList ignoreList) {
        return new TreeFlattener()
                .onlySelected()
                .useIgnoreList(ignoreList)
                .onlyTypes(settings.getAllowedTypes())
                .flatten(root);
    }

    private PgCodeKeeperApi() {
        // only statics
    }
}
