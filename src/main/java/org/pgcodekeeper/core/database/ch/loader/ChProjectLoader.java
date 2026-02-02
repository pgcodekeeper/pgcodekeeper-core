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
package org.pgcodekeeper.core.database.ch.loader;

import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.base.loader.AbstractDumpLoader;
import org.pgcodekeeper.core.database.base.loader.AbstractProjectLoader;
import org.pgcodekeeper.core.database.ch.project.ChWorkDirs;
import org.pgcodekeeper.core.database.ch.schema.ChDatabase;
import org.pgcodekeeper.core.ignorelist.IgnoreSchemaList;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.utils.Utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * ClickHouse project loader for loading database schemas from project directory structures.
 */
public class ChProjectLoader extends AbstractProjectLoader<ChDatabase> {

    /**
     * Creates a new ClickHouse project loader with full configuration.
     *
     * @param dirPath          path to the project directory
     * @param settings         loader settings and configuration
     * @param monitor          progress monitor for tracking loading progress (can be null)
     * @param ignoreSchemaList list of schemas to ignore during loading (can be null)
     */
    public ChProjectLoader(Path dirPath, ISettings settings,
                           IMonitor monitor, IgnoreSchemaList ignoreSchemaList) {
        super(dirPath, settings, monitor, ignoreSchemaList);
    }

    @Override
    protected ChDatabase createDatabase() {
        return new ChDatabase();
    }

    @Override
    protected AbstractDumpLoader<ChDatabase> createDumpLoader(Path file) {
        return new ChDumpLoader(file, settings, monitor);
    }

    @Override
    protected void loadStructure(Path dir, ChDatabase db) throws IOException {
        for (String dirName : ChWorkDirs.getDirectoryNames()) {
            if (ChWorkDirs.DATABASE.equals(dirName)) {
                loadDatabaseStructure(dir, db, dirName);
            } else {
                loadSubdir(dir, dirName, db);
            }
        }
    }

    private void loadDatabaseStructure(Path baseDir, ChDatabase db, String commonDir) throws IOException {
        Path databasesCommonDir = baseDir.resolve(commonDir);
        if (!Files.isDirectory(databasesCommonDir)) {
            return;
        }

        try (Stream<Path> databases = Files.list(databasesCommonDir)) {
            for (Path databaseDir : Utils.streamIterator(databases)) {
                if (Files.isDirectory(databaseDir) && isAllowedSchema(databaseDir.getFileName().toString())) {
                    loadSubdir(databasesCommonDir, databaseDir.getFileName().toString(), db);
                    for (DbObjType dirSub : ChWorkDirs.getDirLoadOrder()) {
                        loadSubdir(databaseDir, dirSub.name(), db);
                    }
                }
            }
        }
    }
}
