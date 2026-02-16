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

import org.pgcodekeeper.core.database.api.jdbc.IJdbcConnector;
import org.pgcodekeeper.core.database.base.loader.AbstractLibraryLoader;
import org.pgcodekeeper.core.database.ms.jdbc.MsJdbcConnector;
import org.pgcodekeeper.core.database.ms.schema.MsDatabase;
import org.pgcodekeeper.core.settings.DiffSettings;

import java.nio.file.Path;
import java.util.Set;

public class MsLibraryLoader extends AbstractLibraryLoader<MsDatabase> {

    public MsLibraryLoader(MsDatabase database, Path metaPath, Set<String> loadedPaths,
                           DiffSettings diffSettings) {
        super(database, metaPath, loadedPaths, diffSettings);
    }

    @Override
    protected MsDatabase createDatabase() {
        return new MsDatabase();
    }

    @Override
    protected MsDumpLoader getDumpLoader(Path p, DiffSettings diffSettings) {
        return new MsDumpLoader(p, diffSettings);
    }

    @Override
    protected MsJdbcLoader createJdbcLoader(String url, DiffSettings diffSettings) {
        IJdbcConnector con = new MsJdbcConnector(url);
        return new MsJdbcLoader(con, diffSettings);
    }

    @Override
    protected MsProjectLoader getProjectLoader(Path p, DiffSettings diffSettings) {
        return new MsProjectLoader(p, diffSettings);
    }

    @Override
    protected MsLibraryLoader getCopy(MsDatabase db) {
        return new MsLibraryLoader(db, metaPath, loadedPaths, diffSettings);
    }
}
