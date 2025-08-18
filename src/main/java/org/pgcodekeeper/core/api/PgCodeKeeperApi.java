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

import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.PgCodekeeperException;
import org.pgcodekeeper.core.PgDiff;
import org.pgcodekeeper.core.ignoreparser.IgnoreParser;
import org.pgcodekeeper.core.model.difftree.*;
import org.pgcodekeeper.core.model.exporter.ModelExporter;
import org.pgcodekeeper.core.schema.AbstractDatabase;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.utils.ProjectUpdater;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

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
    public static String diff(ISettings settings, AbstractDatabase oldDb, AbstractDatabase newDb)
            throws PgCodekeeperException, IOException, InterruptedException {
        return diff(settings, oldDb, newDb, Collections.emptyList());
    }

    /**
     * Compares two databases and generates a migration script with filtering.
     *
     * @param settings       ISettings object
     * @param oldDb          the old database version to compare from
     * @param newDb          the new database version to compare to
     * @param ignoreLists    collection of paths to files containing objects to ignore
     * @return the generated migration script as a string
     * @throws IOException          if I/O operations fail or ignore list file cannot be read
     * @throws InterruptedException if the thread is interrupted during the operation
     */
    public static String diff(ISettings settings, AbstractDatabase oldDb, AbstractDatabase newDb,
                              Collection<String> ignoreLists) throws IOException, InterruptedException {
        IgnoreList ignoreList = IgnoreParser.parseLists(ignoreLists);
        return new PgDiff(settings).diff(oldDb, newDb, ignoreList);
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
    public static void export(ISettings settings, AbstractDatabase dbToExport, String exportTo)
            throws PgCodekeeperException, IOException, InterruptedException {
        export(settings, dbToExport, exportTo, Collections.emptyList(), null);
    }

    /**
     * Exports database schema to project files with filtering and progress tracking.
     *
     * @param settings       ISettings object
     * @param dbToExport     the database to export
     * @param exportTo       path to the target project directory
     * @param ignoreLists    collection of paths to files containing objects to ignore
     * @param monitor        progress monitor for tracking the operation
     * @throws IOException          if I/O operations fail, if export directory does not exist,
     *                              if export directory is not empty, if path is a file,
     *                              or if ignore list file cannot be read
     * @throws InterruptedException if the thread is interrupted during the operation
     */
    public static void export(ISettings settings, AbstractDatabase dbToExport, String exportTo,
                              Collection<String> ignoreLists, IMonitor monitor)
            throws IOException, InterruptedException {
        IgnoreList ignoreList = IgnoreParser.parseLists(ignoreLists);
        TreeElement root;
        root = DiffTree.create(settings, dbToExport, null, monitor);
        root.setAllChecked();

        List<TreeElement> selected = getSelectedElements(settings, root, ignoreList);
        new ModelExporter(Paths.get(exportTo), dbToExport, null, settings.getDbType(), selected,
                Consts.UTF_8, settings).exportProject();
    }

    /**
     * Updates project with changes from database.
     *
     * @param settings        ISettings object
     * @param oldDb           the old database version
     * @param newDb           the new database version with changes
     * @param projectToUpdate path to the project directory to update
     * @throws PgCodekeeperException if update operation fails or if parsing errors occur
     * @throws IOException           if I/O operations fail, if project directory does not exist or if path is a file
     * @throws InterruptedException  if the thread is interrupted during the operation
     */
    public static void update(ISettings settings, AbstractDatabase oldDb, AbstractDatabase newDb, String projectToUpdate)
            throws PgCodekeeperException, IOException, InterruptedException {
        update(settings, oldDb, newDb, projectToUpdate, Collections.emptyList(), null);
    }

    /**
     * Updates project with changes from database with filtering and progress tracking.
     *
     * @param settings             ISettings object
     * @param oldDb                the old database version
     * @param newDb                the new database version with changes
     * @param projectToUpdate      path to the project directory to update
     * @param ignoreLists          collection of paths to files containing objects to ignore
     * @param monitor              progress monitor for tracking the operation
     * @throws PgCodekeeperException if update operation fails or if parsing errors occur
     * @throws IOException           if I/O operations fail, if project directory does not exist, if path is a file,
     *                               or if ignore list files cannot be read
     * @throws InterruptedException  if the thread is interrupted during the operation
     */
    public static void update(ISettings settings, AbstractDatabase oldDb, AbstractDatabase newDb,
                              String projectToUpdate, Collection<String> ignoreLists, IMonitor monitor)
            throws PgCodekeeperException, IOException, InterruptedException {
        IgnoreList ignoreList = IgnoreParser.parseLists(ignoreLists);
        TreeElement root = DiffTree.create(settings, oldDb, newDb, monitor);
        root.setAllChecked();
        List<TreeElement> selected = getSelectedElements(settings, root, ignoreList);

        new ProjectUpdater(newDb, oldDb, selected, settings.getDbType(), Consts.UTF_8, Paths.get(projectToUpdate),
                false, settings).updatePartial();
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
