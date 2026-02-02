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
package org.pgcodekeeper.core.database.ms.project;

import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.api.schema.ISearchPath;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.database.api.schema.ISubElement;
import org.pgcodekeeper.core.database.base.project.AbstractModelExporter;
import org.pgcodekeeper.core.model.difftree.TreeElement;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.utils.FileUtils;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;

/**
 * Model exporter for MS SQL Server databases.
 * Handles MS SQL-specific directory structure and file naming,
 * including schema prefix in filenames for schema-qualified objects.
 */
public class MsModelExporter extends AbstractModelExporter {

    /**
     * Creates a new MsModelExporter for full database export.
     *
     * @param outDir      output directory, should be empty or not exist
     * @param db          database to export
     * @param sqlEncoding SQL file encoding
     * @param settings    export settings
     */
    public MsModelExporter(Path outDir, IDatabase db, String sqlEncoding, ISettings settings) {
        super(outDir, db, sqlEncoding, settings);
    }

    /**
     * Creates a new MsModelExporter for partial or project export.
     *
     * @param outDir         output directory
     * @param newDb          new database schema
     * @param oldDb          old database schema, can be null for project export
     * @param changedObjects collection of changed objects
     * @param sqlEncoding    SQL file encoding
     * @param settings       export settings
     */
    public MsModelExporter(Path outDir, IDatabase newDb, IDatabase oldDb,
                           Collection<TreeElement> changedObjects,
                           String sqlEncoding, ISettings settings) {
        super(outDir, newDb, oldDb, changedObjects, sqlEncoding, settings);
    }

    @Override
    protected List<String> getDirectoryNames() {
        return MsWorkDirs.getDirectoryNames();
    }

    @Override
    protected Path getRelativeFolderPath(IStatement st) {
        return MsWorkDirs.getRelativeFolderPath(st, Paths.get(""));
    }

    @Override
    public Path getRelativeFilePath(IStatement st) {
        if (st instanceof ISubElement) {
            st = st.getParent();
        }
        Path path = getRelativeFolderPath(st);
        String fileName = getExportedFilenameSql(getExportedFilename(st));
        if (st instanceof ISearchPath sp) {
            fileName = FileUtils.getValidFilename(sp.getSchemaName()) + '.' + fileName;
        }
        return path.resolve(fileName);
    }
}
