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
package org.pgcodekeeper.core.database.pg.project;

import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.base.project.AbstractModelExporter;
import org.pgcodekeeper.core.database.base.project.AbstractProjectUpdater;
import org.pgcodekeeper.core.model.difftree.TreeElement;
import org.pgcodekeeper.core.settings.ISettings;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

/**
 * Project updater for PostgreSQL databases.
 * Handles PostgreSQL-specific directory structure and model exporters.
 */
public class PgProjectUpdater extends AbstractProjectUpdater {

    /**
     * Creates a new PostgreSQL project updater with specified configuration.
     *
     * @param dbNew          the new database schema
     * @param dbOld          the old database schema for comparison
     * @param changedObjects collection of changed tree elements
     * @param encoding       the file encoding to use
     * @param dirExport      the export directory path
     * @param overridesOnly  whether to update only overrides
     * @param settings       the application settings
     */
    public PgProjectUpdater(IDatabase dbNew, IDatabase dbOld, Collection<TreeElement> changedObjects,
                            String encoding, Path dirExport, boolean overridesOnly, ISettings settings) {
        super(dbNew, dbOld, changedObjects, encoding, dirExport, overridesOnly, settings);
    }

    @Override
    protected List<String> getDirectoryNames() {
        return PgWorkDirs.getDirectoryNames();
    }

    @Override
    protected AbstractModelExporter createModelExporter(Path outDir, IDatabase db, String sqlEncoding) {
        return new PgModelExporter(outDir, db, sqlEncoding, settings);
    }

    @Override
    protected AbstractModelExporter createModelExporter(Path outDir, IDatabase newDb, IDatabase oldDb,
                                                        Collection<TreeElement> changedObjects, String sqlEncoding) {
        return new PgModelExporter(outDir, newDb, oldDb, changedObjects, sqlEncoding, settings);
    }

    @Override
    protected AbstractModelExporter createOverridesModelExporter(Path outDir, IDatabase newDb, IDatabase oldDb,
                                                                 Collection<TreeElement> changedObjects,
                                                                 String sqlEncoding) {
        return new PgOverridesModelExporter(outDir, newDb, oldDb, changedObjects, sqlEncoding, settings);
    }
}
