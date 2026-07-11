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

import org.pgcodekeeper.core.database.base.loader.AbstractDumpLoader;
import org.pgcodekeeper.core.database.base.loader.AbstractLibraryLoader;
import org.pgcodekeeper.core.database.base.loader.AbstractProjectLoader;
import org.pgcodekeeper.core.database.base.project.AbstractWorkDirs;
import org.pgcodekeeper.core.database.ch.project.ChWorkDirs;
import org.pgcodekeeper.core.database.ch.schema.ChDatabase;
import org.pgcodekeeper.core.settings.ISettings;

import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;

/**
 * ClickHouse project loader for loading database schemas from project directory structures.
 */
public class ChProjectLoader extends AbstractProjectLoader<ChDatabase> {

    public ChProjectLoader(Path dirPath, ISettings settings) {
        super(dirPath, settings, new ChWorkDirs(AbstractWorkDirs.resolveAltDirsFile(dirPath)));
    }

    public ChProjectLoader(Path dirPath, ISettings settings, Collection<String> libXmls,
                           Collection<String> libs, Collection<String> libsWithoutPriv, Path metaPath) {
        super(dirPath, settings, new ChWorkDirs(AbstractWorkDirs.resolveAltDirsFile(dirPath)),
                libXmls, libs, libsWithoutPriv, metaPath);
    }

    @Override
    protected ChDatabase createDatabase() {
        return new ChDatabase();
    }

    @Override
    protected AbstractDumpLoader<ChDatabase> createDumpLoader(Path file) {
        return new ChDumpLoader(file, settings);
    }

    @Override
    protected AbstractLibraryLoader<ChDatabase> createLibraryLoader(ChDatabase db) {
        return new ChLibraryLoader(db, metaPath, new HashSet<>(), settings);
    }
}
