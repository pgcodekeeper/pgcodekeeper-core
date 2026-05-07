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
package org.pgcodekeeper.core.database.pg.loader;

import org.pgcodekeeper.core.database.base.loader.AbstractDumpLoader;
import org.pgcodekeeper.core.database.base.loader.AbstractLibraryLoader;
import org.pgcodekeeper.core.database.base.loader.AbstractProjectLoader;
import org.pgcodekeeper.core.database.base.project.AbstractWorkDirs;
import org.pgcodekeeper.core.database.pg.project.PgWorkDirs;
import org.pgcodekeeper.core.database.pg.schema.PgDatabase;
import org.pgcodekeeper.core.settings.DiffSettings;

import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;

/**
 * PostgreSQL project loader for loading database schemas from project directory structures.
 */
public class PgProjectLoader extends AbstractProjectLoader<PgDatabase> {

    public PgProjectLoader(Path dirPath, DiffSettings diffSettings) {
        super(dirPath, diffSettings, new PgWorkDirs(AbstractWorkDirs.resolveAltDirsFile(dirPath)));
    }

    public PgProjectLoader(Path dirPath, DiffSettings diffSettings, Collection<String> libXmls,
                           Collection<String> libs, Collection<String> libsWithoutPriv, Path metaPath) {
        super(dirPath, diffSettings, new PgWorkDirs(AbstractWorkDirs.resolveAltDirsFile(dirPath)),
                libXmls, libs, libsWithoutPriv, metaPath);
    }

    @Override
    protected PgDatabase createDatabase() {
        return new PgDatabase();
    }

    @Override
    protected AbstractDumpLoader<PgDatabase> createDumpLoader(Path file) {
        return new PgDumpLoader(file, diffSettings);
    }

    @Override
    protected AbstractLibraryLoader<PgDatabase> createLibraryLoader(PgDatabase db) {
        return new PgLibraryLoader(db, metaPath, new HashSet<>(), diffSettings);
    }
}
