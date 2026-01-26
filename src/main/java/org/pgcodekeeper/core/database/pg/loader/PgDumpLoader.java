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

import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.database.base.loader.AbstractDumpLoader;
import org.pgcodekeeper.core.database.base.parser.AntlrTask;
import org.pgcodekeeper.core.database.base.schema.AbstractSchema;
import org.pgcodekeeper.core.database.pg.parser.*;
import org.pgcodekeeper.core.database.pg.schema.PgDatabase;
import org.pgcodekeeper.core.database.pg.schema.PgSchema;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.utils.InputStreamProvider;

import java.nio.file.Path;
import java.util.Queue;

/**
 * PostgreSQL dump loader
 */
public class PgDumpLoader extends AbstractDumpLoader<PgDatabase> {

    public PgDumpLoader(InputStreamProvider input, String inputObjectName, ISettings settings) {
        super(input, inputObjectName, settings);
    }

    public PgDumpLoader(Path inputFile, ISettings settings, IMonitor monitor) {
        super(inputFile, settings, monitor);
    }

    public PgDumpLoader(Path inputFile, ISettings settings) {
        super(inputFile, settings);
    }

    @Override
    protected PgDatabase createDatabase() {
        return new PgDatabase();
    }

    @Override
    protected AbstractSchema createDefaultSchema() {
        return new PgSchema(Consts.PUBLIC);
    }

    @Override
    public void loadWithoutAnalyze(PgDatabase db, Queue<AntlrTask<?>> antlrTasks) {
        IPgContextProcessor listener;
        if (overrides != null) {
            listener = new PgOverridesListener(db, inputObjectName, mode, errors,
                    monitor, overrides, settings);
        } else {
            listener = new PgCustomParserListener(db, inputObjectName, mode, errors,
                    antlrTasks, monitor, settings);
        }
        PgParserUtils.parseSqlStream(input, settings.getInCharsetName(), inputObjectName,
                errors, monitor, monitoringLevel, listener, antlrTasks);
    }
}
