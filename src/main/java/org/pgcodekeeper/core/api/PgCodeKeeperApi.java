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
import org.pgcodekeeper.core.model.difftree.DiffTree;
import org.pgcodekeeper.core.model.difftree.TreeElement;
import org.pgcodekeeper.core.model.difftree.TreeFlattener;
import org.pgcodekeeper.core.settings.DiffSettings;
import org.pgcodekeeper.core.settings.ISettings;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Main API class for pgCodeKeeper database operations.
 */
public final class PgCodeKeeperApi {

    /**
     * Compares two databases and generates a migration script.
     *
     * @param oldDb        the old database version to compare from
     * @param newDb        the new database version to compare to
     * @param diffSettings unified context object containing settings, ignore list, and error accumulator
     * @return the generated migration script as a string
     * @throws IOException          if I/O operations fail
     * @throws InterruptedException if the thread is interrupted during the operation
     */
    public static String diff(IDatabase oldDb,
                              IDatabase newDb,
                              DiffSettings diffSettings)
            throws IOException, InterruptedException {
        TreeElement root = DiffTree.create(diffSettings.getSettings(), oldDb, newDb);
        root.setAllChecked();
        return diff(root, oldDb, newDb, diffSettings);
    }

    /**
     * Compares two databases and generates a migration script with a pre-built tree.
     *
     * @param root         root element of tree
     * @param oldDb        the old database version to compare from
     * @param newDb        the new database version to compare to
     * @param diffSettings unified context object containing settings, ignore list, and error accumulator
     * @return the generated migration script as a string
     * @throws IOException if I/O operations fail
     */
    public static String diff(TreeElement root,
                              IDatabase oldDb,
                              IDatabase newDb,
                              DiffSettings diffSettings)
            throws IOException {
        PgDiff pgDiff = new PgDiff(diffSettings);
        String result = pgDiff.diff(root, oldDb, newDb);
        diffSettings.addErrors(pgDiff.getErrors());
        return result;
    }

    /**
     * Exports database schema to project files.
     *
     * @param dbToExport   the database to export
     * @param exportTo     path to the target project directory
     * @param diffSettings unified context object containing settings, monitor, ignore list, and error accumulator
     * @throws IOException          if I/O operations fail, if export directory does not exist,
     *                              if export directory is not empty or if path is a file
     * @throws InterruptedException if the thread is interrupted during the operation
     */
    public static void export(IDatabase dbToExport, Path exportTo, DiffSettings diffSettings)
            throws IOException, InterruptedException {
        IgnoreList ignoreList = diffSettings.getIgnoreList();
        ISettings settings = diffSettings.getSettings();
        TreeElement root = DiffTree.create(settings, dbToExport, null, diffSettings.getMonitor());
        root.setAllChecked();

        List<TreeElement> selected = getSelectedElements(settings, root, ignoreList);
        createModelExporter(exportTo, dbToExport, selected, settings).exportProject();
    }

    /**
     * Updates project with changes from database.
     *
     * @param oldDb           the old database version
     * @param newDb           the new database version with changes
     * @param projectToUpdate path to the project directory to update
     * @param diffSettings    unified context object containing settings, monitor, ignore list, and error accumulator
     * @throws IOException          if I/O operations fail, if project directory does not exist or if path is a file
     * @throws InterruptedException if the thread is interrupted during the operation
     */
    public static void update(IDatabase oldDb, IDatabase newDb, Path projectToUpdate, DiffSettings diffSettings)
            throws IOException, InterruptedException {
        IgnoreList ignoreList = diffSettings.getIgnoreList();
        ISettings settings = diffSettings.getSettings();
        TreeElement root = DiffTree.create(settings, oldDb, newDb, diffSettings.getMonitor());
        root.setAllChecked();
        List<TreeElement> selected = getSelectedElements(settings, root, ignoreList);

        createProjectUpdater(newDb, oldDb, selected, projectToUpdate, settings).updatePartial();
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
