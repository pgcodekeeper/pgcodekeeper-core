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
package org.pgcodekeeper.core.database.base.loader;

import org.pgcodekeeper.core.database.api.loader.IDumpLoader;
import org.pgcodekeeper.core.database.api.parser.ParserListenerMode;
import org.pgcodekeeper.core.database.api.project.IWorkDirs;
import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.parser.AntlrTask;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.database.base.schema.StatementOverride;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.utils.InputStreamProvider;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Queue;

/**
 * Base database dump loader
 */
public abstract class AbstractDumpLoader<T extends IDatabase> extends AbstractLoader<T> implements IDumpLoader {

    protected final InputStreamProvider input;
    protected final int monitoringLevel;
    protected ParserListenerMode mode = ParserListenerMode.NORMAL;
    protected Map<AbstractStatement, StatementOverride> overrides;
    protected IWorkDirs workDirs;

    protected AbstractDumpLoader(InputStreamProvider input, String databaseName,
                                 ISettings settings, int monitoringLevel) {
        super(settings, databaseName);
        this.input = input;
        this.monitoringLevel = monitoringLevel;
    }

    protected AbstractDumpLoader(InputStreamProvider input, String dbOriginName, ISettings settings) {
        this(input, dbOriginName, settings, 0);
    }

    protected AbstractDumpLoader(Path inputFile, ISettings settings) {
        this(() -> Files.newInputStream(inputFile), inputFile.toString(), settings, 1);
    }

    @Override
    public T loadInternal() throws IOException, InterruptedException {
        var db = createDatabaseWithSchema();
        IMonitor.checkCancelled(getMonitor());
        loadWithoutAnalyze(db, antlrTasks);
        finishLoaders();
        return db;
    }

    public T createDatabaseWithSchema() {
        T db = createDatabase();
        ISchema schema = createDefaultSchema();
        db.addChild(schema);
        ObjectLocation loc = new ObjectLocation.Builder()
                .setReference(new ObjectReference(schema.getName(), DbObjType.SCHEMA))
                .build();
        schema.setLocation(loc);
        db.setDefaultSchema(schema.getName());
        return db;
    }

    protected abstract ISchema createDefaultSchema();

    public abstract void loadWithoutAnalyze(T db, Queue<AntlrTask<?>> antlrTasks);

    @Override
    public void setMode(ParserListenerMode mode) {
        this.mode = mode;
    }

    public void setOverridesMap(Map<AbstractStatement, StatementOverride> overrides) {
        this.overrides = overrides;
    }

    public void setWorkDirs(IWorkDirs workDirs) {
        this.workDirs = workDirs;
    }
}
