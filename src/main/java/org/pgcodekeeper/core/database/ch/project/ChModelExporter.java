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
package org.pgcodekeeper.core.database.ch.project;

import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.database.base.project.AbstractModelExporter;
import org.pgcodekeeper.core.model.difftree.TreeElement;
import org.pgcodekeeper.core.settings.ISettings;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;

/**
 * Model exporter for ClickHouse databases.
 * Handles ClickHouse-specific directory structure and file naming.
 */
public class ChModelExporter extends AbstractModelExporter {

    /**
     * Creates a new ChModelExporter for full database export.
     *
     * @param outDir      output directory, should be empty or not exist
     * @param db          database to export
     * @param sqlEncoding SQL file encoding
     * @param settings    export settings
     */
    public ChModelExporter(Path outDir, IDatabase db, String sqlEncoding, ISettings settings) {
        super(outDir, db, sqlEncoding, settings);
    }

    /**
     * Creates a new ChModelExporter for partial or project export.
     *
     * @param outDir         output directory
     * @param newDb          new database schema
     * @param oldDb          old database schema, can be null for project export
     * @param changedObjects collection of changed objects
     * @param sqlEncoding    SQL file encoding
     * @param settings       export settings
     */
    public ChModelExporter(Path outDir, IDatabase newDb, IDatabase oldDb,
                           Collection<TreeElement> changedObjects,
                           String sqlEncoding, ISettings settings) {
        super(outDir, newDb, oldDb, changedObjects, sqlEncoding, settings);
    }

    @Override
    protected List<String> getDirectoryNames() {
        return ChWorkDirs.getDirectoryNames();
    }

    @Override
    protected Path getRelativeFolderPath(IStatement st) {
        return ChWorkDirs.getRelativeFolderPath(st, Paths.get(""));
    }
}
