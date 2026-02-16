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
import org.pgcodekeeper.core.database.base.parser.AntlrTask;
import org.pgcodekeeper.core.database.pg.parser.IPgContextProcessor;
import org.pgcodekeeper.core.database.pg.parser.PgCustomParserListener;
import org.pgcodekeeper.core.database.pg.parser.PgOverridesListener;
import org.pgcodekeeper.core.database.pg.parser.PgParserUtils;
import org.pgcodekeeper.core.database.pg.schema.PgDatabase;
import org.pgcodekeeper.core.database.pg.schema.PgSchema;
import org.pgcodekeeper.core.database.pg.utils.PgConsts;
import org.pgcodekeeper.core.settings.DiffSettings;
import org.pgcodekeeper.core.utils.InputStreamProvider;

import java.nio.file.Path;
import java.util.Queue;

/**
 * PostgreSQL dump loader
 */
public class PgDumpLoader extends AbstractDumpLoader<PgDatabase> {

    public PgDumpLoader(InputStreamProvider input, String inputObjectName, DiffSettings diffSettings) {
        super(input, inputObjectName, diffSettings);
    }

    public PgDumpLoader(Path inputFile, DiffSettings diffSettings) {
        super(inputFile, diffSettings);
    }

    @Override
    protected PgDatabase createDatabase() {
        return new PgDatabase();
    }

    @Override
    protected PgSchema createDefaultSchema() {
        return new PgSchema(PgConsts.DEFAULT_SCHEMA);
    }

    @Override
    public void loadWithoutAnalyze(PgDatabase db, Queue<AntlrTask<?>> antlrTasks) {
        IPgContextProcessor listener;
        if (overrides != null) {
            listener = new PgOverridesListener(db, inputObjectName, mode, diffSettings, overrides);
        } else {
            listener = new PgCustomParserListener(db, inputObjectName, mode, diffSettings, antlrTasks);
        }
        PgParserUtils.parseSqlStream(input, inputObjectName, diffSettings, monitoringLevel, listener, antlrTasks);
    }
}
