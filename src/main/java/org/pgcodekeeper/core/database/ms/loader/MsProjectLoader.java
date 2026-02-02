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

import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.api.schema.GenericColumn;
import org.pgcodekeeper.core.database.api.schema.ObjectLocation;
import org.pgcodekeeper.core.database.base.loader.AbstractDumpLoader;
import org.pgcodekeeper.core.database.base.loader.AbstractProjectLoader;
import org.pgcodekeeper.core.database.base.parser.AntlrTaskManager;
import org.pgcodekeeper.core.database.ms.project.MsWorkDirs;
import org.pgcodekeeper.core.database.ms.schema.MsDatabase;
import org.pgcodekeeper.core.database.ms.schema.MsSchema;
import org.pgcodekeeper.core.ignorelist.IgnoreSchemaList;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.settings.ISettings;

import java.io.IOException;
import java.nio.file.Path;

/**
 * MS SQL Server project loader for loading database schemas from project directory structures.
 */
public class MsProjectLoader extends AbstractProjectLoader<MsDatabase> {

    /**
     * Creates a new MS SQL project loader with full configuration.
     *
     * @param dirPath          path to the project directory
     * @param settings         loader settings and configuration
     * @param monitor          progress monitor for tracking loading progress (can be null)
     * @param ignoreSchemaList list of schemas to ignore during loading (can be null)
     */
    public MsProjectLoader(Path dirPath, ISettings settings,
                           IMonitor monitor, IgnoreSchemaList ignoreSchemaList) {
        super(dirPath, settings, monitor, ignoreSchemaList);
    }

    @Override
    protected MsDatabase createDatabase() {
        return new MsDatabase();
    }

    @Override
    protected AbstractDumpLoader<MsDatabase> createDumpLoader(Path file) {
        return new MsDumpLoader(file, settings, monitor);
    }

    @Override
    protected void loadStructure(Path dir, MsDatabase db) throws InterruptedException, IOException {
        Path securityFolder = dir.resolve(MsWorkDirs.SECURITY);

        loadSubdir(securityFolder, MsWorkDirs.SCHEMAS, db, this::isAllowedSchema);
        // DBO schema check requires schema loads to finish first
        AntlrTaskManager.finish(antlrTasks);
        collectDumpLoaderErrors();
        addDboSchema(db);

        loadSubdir(securityFolder, MsWorkDirs.ROLES, db);
        loadSubdir(securityFolder, MsWorkDirs.USERS, db);

        for (String dirSub : MsWorkDirs.getDirectoryNames()) {
            if (MsWorkDirs.isInSchema(dirSub)) {
                // get schema name from file names and filter
                loadSubdir(dir, dirSub, db, this::isAllowedSchema);
                continue;
            }
            loadSubdir(dir, dirSub, db);
        }
    }

    private void addDboSchema(MsDatabase db) {
        if (!db.containsSchema(Consts.DBO)) {
            MsSchema schema = new MsSchema(Consts.DBO);
            ObjectLocation loc = new ObjectLocation.Builder()
                    .setObject(new GenericColumn(Consts.DBO, DbObjType.SCHEMA))
                    .build();

            schema.setLocation(loc);
            db.addSchema(schema);
            db.setDefaultSchema(Consts.DBO);
        }
    }

    private void collectDumpLoaderErrors() {
        dumpLoaders.forEach(l -> errors.addAll(l.getErrors()));
        dumpLoaders.clear();
    }
}
