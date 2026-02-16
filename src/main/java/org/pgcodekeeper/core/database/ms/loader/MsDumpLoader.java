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

import org.pgcodekeeper.core.database.base.loader.AbstractDumpLoader;
import org.pgcodekeeper.core.database.base.parser.AntlrTask;
import org.pgcodekeeper.core.database.ms.parser.IMsContextProcessor;
import org.pgcodekeeper.core.database.ms.parser.MsCustomParserListener;
import org.pgcodekeeper.core.database.ms.parser.MsOverridesListener;
import org.pgcodekeeper.core.database.ms.parser.MsParserUtils;
import org.pgcodekeeper.core.database.ms.schema.MsDatabase;
import org.pgcodekeeper.core.database.ms.schema.MsSchema;
import org.pgcodekeeper.core.database.ms.utils.MsConsts;
import org.pgcodekeeper.core.settings.DiffSettings;
import org.pgcodekeeper.core.utils.InputStreamProvider;

import java.nio.file.Path;
import java.util.Queue;

/**
 * MS SQL dump loader
 */
public class MsDumpLoader extends AbstractDumpLoader<MsDatabase> {

    public MsDumpLoader(InputStreamProvider input, String inputObjectName, DiffSettings diffSettings) {
        super(input, inputObjectName, diffSettings);
    }

    public MsDumpLoader(Path inputFile, DiffSettings diffSettings) {
        super(inputFile, diffSettings);
    }

    @Override
    protected MsDatabase createDatabase() {
        return new MsDatabase();
    }

    @Override
    protected MsSchema createDefaultSchema() {
        return new MsSchema(MsConsts.DEFAULT_SCHEMA);
    }

    @Override
    public void loadWithoutAnalyze(MsDatabase db, Queue<AntlrTask<?>> antlrTasks) {
        IMsContextProcessor listener;
        if (overrides != null) {
            listener = new MsOverridesListener(db, inputObjectName, mode, diffSettings, overrides);
        } else {
            listener = new MsCustomParserListener(db, inputObjectName, mode, diffSettings);
        }
        MsParserUtils.parseSqlStream(input, inputObjectName, diffSettings, monitoringLevel, listener, antlrTasks);
    }
}
