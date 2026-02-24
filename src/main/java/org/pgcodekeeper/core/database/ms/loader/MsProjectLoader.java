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
package org.pgcodekeeper.core.database.ms.loader;

import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.api.schema.ObjectLocation;
import org.pgcodekeeper.core.database.api.schema.ObjectReference;
import org.pgcodekeeper.core.database.base.loader.AbstractDumpLoader;
import org.pgcodekeeper.core.database.base.loader.AbstractLibraryLoader;
import org.pgcodekeeper.core.database.base.loader.AbstractProjectLoader;
import org.pgcodekeeper.core.database.base.parser.AntlrTaskManager;
import org.pgcodekeeper.core.database.ms.project.MsWorkDirs;
import org.pgcodekeeper.core.database.ms.schema.MsDatabase;
import org.pgcodekeeper.core.database.ms.schema.MsSchema;
import org.pgcodekeeper.core.database.ms.utils.MsConsts;
import org.pgcodekeeper.core.settings.DiffSettings;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;

/**
 * MS SQL Server project loader for loading database schemas from project directory structures.
 */
public class MsProjectLoader extends AbstractProjectLoader<MsDatabase> {

    public MsProjectLoader(Path dirPath, DiffSettings diffSettings) {
        super(dirPath, diffSettings);
    }

    public MsProjectLoader(Path dirPath, DiffSettings diffSettings, Collection<String> libXmls,
                           Collection<String> libs, Collection<String> libsWithoutPriv, Path metaPath) {
        super(dirPath, diffSettings, libXmls, libs, libsWithoutPriv, metaPath);
    }

    @Override
    protected MsDatabase createDatabase() {
        return new MsDatabase();
    }

    @Override
    protected AbstractDumpLoader<MsDatabase> createDumpLoader(Path file) {
        return new MsDumpLoader(file, diffSettings);
    }

    @Override
    protected AbstractLibraryLoader<MsDatabase> createLibraryLoader(MsDatabase db) {
        return new MsLibraryLoader(db, metaPath, new HashSet<>(), diffSettings);
    }

    @Override
    protected void loadStructure(Path dir, MsDatabase db) throws InterruptedException, IOException {
        Path securityFolder = dir.resolve(MsWorkDirs.SECURITY);

        loadSubdir(securityFolder, MsWorkDirs.SCHEMAS, db, fileName -> isAllowedSchema(fileName.split("\\.")[0]));
        // DBO schema check requires schema loads to finish first
        AntlrTaskManager.finish(antlrTasks);
        collectDumpLoaderErrors();
        addDboSchema(db);

        loadSubdir(securityFolder, MsWorkDirs.ROLES, db);
        loadSubdir(securityFolder, MsWorkDirs.USERS, db);

        for (String dirSub : MsWorkDirs.getDirectoryNames()) {
            if (MsWorkDirs.isInSchema(dirSub)) {
                // get schema name from file names (format: schema.objectname.sql) and filter
                loadSubdir(dir, dirSub, db, fileName -> isAllowedSchema(fileName.split("\\.")[0]));
                continue;
            }
            loadSubdir(dir, dirSub, db);
        }
    }

    private void addDboSchema(MsDatabase db) {
        if (!db.containsSchema(MsConsts.DEFAULT_SCHEMA)) {
            MsSchema schema = new MsSchema(MsConsts.DEFAULT_SCHEMA);
            ObjectLocation loc = new ObjectLocation.Builder()
                    .setReference(new ObjectReference(MsConsts.DEFAULT_SCHEMA, DbObjType.SCHEMA))
                    .build();

            schema.setLocation(loc);
            db.addSchema(schema);
            db.setDefaultSchema(MsConsts.DEFAULT_SCHEMA);
        }
    }

    private void collectDumpLoaderErrors() {
        dumpLoaders.clear();
    }
}
