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
import org.pgcodekeeper.core.model.difftree.*;
import org.pgcodekeeper.core.model.exporter.ModelExporter;
import org.pgcodekeeper.core.schema.AbstractDatabase;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.utils.IMonitor;
import org.pgcodekeeper.core.utils.NullMonitor;
import org.pgcodekeeper.core.utils.ProjectUpdater;

import java.io.IOException;
import java.nio.file.Paths;
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
        return new PgDiff(settings).diff(oldDb, newDb, null);
    }

    /**
     * Compares two databases and generates a migration script with filtering.
     *
     * @param settings       ISettings object
     * @param oldDb          the old database version to compare from
     * @param newDb          the new database version to compare to
     * @param ignoreListPath path to file containing objects to ignore or null for no filtering
     * @return the generated migration script as a string
     * @throws IOException          if I/O operations fail or ignore list file cannot be read
     * @throws InterruptedException if the thread is interrupted during the operation
     */
    public static String diff(ISettings settings, AbstractDatabase oldDb, AbstractDatabase newDb,
                              String ignoreListPath) throws IOException, InterruptedException {
        var ignoreList = IIgnoreList.parseIgnoreList(ignoreListPath, new IgnoreList());
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
        export(settings, dbToExport, exportTo, null, new NullMonitor());
    }

    /**
     * Exports database schema to project files with filtering and progress tracking.
     *
     * @param settings       ISettings object
     * @param dbToExport     the database to export
     * @param exportTo       path to the target project directory
     * @param ignoreListPath path to file containing objects to ignore or null for no filtering
     * @param monitor        progress monitor for tracking the operation
     * @throws IOException          if I/O operations fail, if export directory does not exist,
     *                              if export directory is not empty, if path is a file,
     *                              or if ignore list file cannot be read
     * @throws InterruptedException if the thread is interrupted during the operation
     */
    public static void export(ISettings settings, AbstractDatabase dbToExport, String exportTo, String ignoreListPath,
                              IMonitor monitor)
            throws IOException, InterruptedException {
        var ignoreList = IIgnoreList.parseIgnoreList(ignoreListPath, new IgnoreList());
        TreeElement root;
        root = DiffTree.create(dbToExport, null, monitor);
        root.setAllChecked();

        List<TreeElement> selected = getSelectedElements(settings, root, ignoreList);
        new ModelExporter(Paths.get(exportTo), dbToExport, null, settings.getDbType(), selected,
                Consts.UTF_8, settings).exportProject();
    }

    /**
     * Updates project with changes from database.
     *
     * @param settings        ISettings object
     * @param newDb           the new database version with changes
     * @param projectToUpdate path to the project directory to update
     * @throws PgCodekeeperException if update operation fails or if parsing errors occur
     * @throws IOException           if I/O operations fail, if project directory does not exist or if path is a file
     * @throws InterruptedException  if the thread is interrupted during the operation
     */
    public static void update(ISettings settings, AbstractDatabase newDb, String projectToUpdate)
            throws PgCodekeeperException, IOException, InterruptedException {
        update(settings, newDb, projectToUpdate, null, null, new NullMonitor());
    }

    /**
     * Updates project with changes from database with filtering and progress tracking.
     *
     * @param settings             ISettings object
     * @param newDb                the new database version with changes
     * @param projectToUpdate      path to the project directory to update
     * @param ignoreListPath       path to file containing objects to ignore or null for no filtering
     * @param ignoreSchemaListPath path to file containing schemas to ignore during project loading or null
     *                             for no filtering
     * @param monitor              progress monitor for tracking the operation
     * @throws PgCodekeeperException if update operation fails or if parsing errors occur
     * @throws IOException           if I/O operations fail, if project directory does not exist, if path is a file,
     *                               or if ignore list files cannot be read
     * @throws InterruptedException  if the thread is interrupted during the operation
     */
    public static void update(ISettings settings, AbstractDatabase newDb, String projectToUpdate,
                              String ignoreListPath, String ignoreSchemaListPath, IMonitor monitor)
            throws PgCodekeeperException, IOException, InterruptedException {
        var ignoreList = IIgnoreList.parseIgnoreList(ignoreListPath, new IgnoreList());
        var oldDb = DatabaseFactory.loadFromProject(settings, projectToUpdate, ignoreSchemaListPath, monitor);

        TreeElement root = DiffTree.create(oldDb, newDb, monitor);
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
