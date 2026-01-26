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

import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.database.base.loader.AbstractDumpLoader;
import org.pgcodekeeper.core.database.base.parser.AntlrTask;
import org.pgcodekeeper.core.database.base.schema.AbstractSchema;
import org.pgcodekeeper.core.database.ch.parser.*;
import org.pgcodekeeper.core.database.ch.schema.ChDatabase;
import org.pgcodekeeper.core.database.ch.schema.ChSchema;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.utils.InputStreamProvider;

import java.nio.file.Path;
import java.util.Queue;

/**
 * ClickHouse dump loader
 */
public class ChDumpLoader extends AbstractDumpLoader<ChDatabase> {

    public ChDumpLoader(InputStreamProvider input, String inputObjectName, ISettings settings) {
        super(input, inputObjectName, settings);
    }

    public ChDumpLoader(Path inputFile, ISettings settings, IMonitor monitor) {
        super(inputFile, settings, monitor);
    }

    public ChDumpLoader(Path inputFile, ISettings settings) {
        super(inputFile, settings);
    }

    @Override
    protected ChDatabase createDatabase() {
        return new ChDatabase();
    }

    @Override
    protected AbstractSchema createDefaultSchema() {
        return new ChSchema(Consts.CH_DEFAULT_DB);
    }

    @Override
    public void loadWithoutAnalyze(ChDatabase db, Queue<AntlrTask<?>> antlrTasks) {
        IChContextProcessor listener;
        if (overrides != null) {
            listener = new ChOverridesListener(db, inputObjectName, mode, errors,
                    monitor, overrides, settings);
        } else {
            listener = new ChCustomParserListener(db, inputObjectName, mode, errors,
                    monitor, settings);
        }
        ChParserUtils.parseSqlStream(input, settings.getInCharsetName(), inputObjectName,
                errors, monitor, monitoringLevel, listener, antlrTasks);
    }
}
