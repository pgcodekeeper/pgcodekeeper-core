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

import org.pgcodekeeper.core.database.api.jdbc.IJdbcConnector;
import org.pgcodekeeper.core.database.base.loader.AbstractLibraryLoader;
import org.pgcodekeeper.core.database.ch.jdbc.ChJdbcConnector;
import org.pgcodekeeper.core.database.ch.schema.ChDatabase;
import org.pgcodekeeper.core.settings.DiffSettings;

import java.nio.file.Path;
import java.util.Set;

public class ChLibraryLoader extends AbstractLibraryLoader<ChDatabase> {

    public ChLibraryLoader(ChDatabase database, Path metaPath, Set<String> loadedPaths,
                           DiffSettings diffSettings) {
        super(database, metaPath, loadedPaths, diffSettings);
    }

    @Override
    protected ChDatabase createDatabase() {
        return new ChDatabase();
    }

    @Override
    protected ChDumpLoader getDumpLoader(Path p, DiffSettings diffSettings) {
        return new ChDumpLoader(p, diffSettings);
    }

    @Override
    protected ChJdbcLoader createJdbcLoader(String url, DiffSettings diffSettings) {
        IJdbcConnector con = new ChJdbcConnector(url);
        return new ChJdbcLoader(con, diffSettings);
    }

    @Override
    protected ChProjectLoader getProjectLoader(Path p, DiffSettings diffSettings) {
        return new ChProjectLoader(p, diffSettings);
    }

    @Override
    protected ChLibraryLoader getCopy(ChDatabase db) {
        return new ChLibraryLoader(db, metaPath, loadedPaths, diffSettings);
    }
}
