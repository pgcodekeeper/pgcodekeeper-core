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
import org.pgcodekeeper.core.database.base.project.AbstractOverridesModelExporter;
import org.pgcodekeeper.core.model.difftree.TreeElement;
import org.pgcodekeeper.core.settings.ISettings;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;

/**
 * Overrides model exporter for ClickHouse databases.
 * Handles ClickHouse-specific directory structure and file naming.
 */
public class ChOverridesModelExporter extends AbstractOverridesModelExporter {

    /**
     * Creates a new ChOverridesModelExporter.
     *
     * @param outDir         output directory for export
     * @param newDb          new database schema
     * @param oldDb          old database schema
     * @param changedObjects collection of changed tree elements
     * @param sqlEncoding    SQL file encoding
     * @param settings       export settings
     */
    public ChOverridesModelExporter(Path outDir, IDatabase newDb, IDatabase oldDb,
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
