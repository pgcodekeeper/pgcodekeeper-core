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
import org.pgcodekeeper.core.database.base.loader.AbstractLibraryLoader;
import org.pgcodekeeper.core.database.base.loader.AbstractProjectLoader;
import org.pgcodekeeper.core.database.ch.project.ChWorkDirs;
import org.pgcodekeeper.core.database.ch.schema.ChDatabase;
import org.pgcodekeeper.core.settings.DiffSettings;
import org.pgcodekeeper.core.utils.Utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;
import java.util.stream.Stream;

/**
 * ClickHouse project loader for loading database schemas from project directory structures.
 */
public class ChProjectLoader extends AbstractProjectLoader<ChDatabase> {

    public ChProjectLoader(Path dirPath, DiffSettings diffSettings) {
        super(dirPath, diffSettings);
    }

    public ChProjectLoader(Path dirPath, DiffSettings diffSettings, Collection<String> libXmls,
                           Collection<String> libs, Collection<String> libsWithoutPriv, Path metaPath) {
        super(dirPath, diffSettings, libXmls, libs, libsWithoutPriv, metaPath);
    }

    @Override
    protected ChDatabase createDatabase() {
        return new ChDatabase();
    }

    @Override
    protected AbstractDumpLoader<ChDatabase> createDumpLoader(Path file) {
        return new ChDumpLoader(file, diffSettings);
    }

    @Override
    protected AbstractLibraryLoader<ChDatabase> createLibraryLoader(ChDatabase db) {
        return new ChLibraryLoader(db, metaPath, new HashSet<>(), diffSettings);
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
