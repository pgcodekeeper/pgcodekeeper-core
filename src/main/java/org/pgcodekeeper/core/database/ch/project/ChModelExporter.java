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
import org.pgcodekeeper.core.database.base.project.AbstractModelExporter;
import org.pgcodekeeper.core.database.base.project.AbstractWorkDirs;
import org.pgcodekeeper.core.model.difftree.TreeElement;
import org.pgcodekeeper.core.settings.ISettings;

import java.nio.file.Path;
import java.util.Collection;

/**
 * Model exporter for ClickHouse databases.
 * Handles ClickHouse-specific directory structure and file naming.
 */
public class ChModelExporter extends AbstractModelExporter {

    public ChModelExporter(Path outDir, IDatabase db, String sqlEncoding, ISettings settings) {
        super(outDir, db, sqlEncoding, settings, new ChWorkDirs(AbstractWorkDirs.resolveAltDirsFile(outDir)));
    }

    public ChModelExporter(Path outDir, IDatabase newDb, IDatabase oldDb,
                           Collection<TreeElement> changedObjects,
                           String sqlEncoding, ISettings settings) {
        super(outDir, newDb, oldDb, changedObjects, sqlEncoding, settings,
                new ChWorkDirs(AbstractWorkDirs.resolveAltDirsFile(outDir)));
    }

    public ChModelExporter(Path outDir, IDatabase newDb, IDatabase oldDb, Path structureFile,
                           Collection<TreeElement> changedObjects,
                           String sqlEncoding, ISettings settings) {
        super(outDir, newDb, oldDb, changedObjects, sqlEncoding, settings, new ChWorkDirs(structureFile),
                structureFile != null);
    }
}
