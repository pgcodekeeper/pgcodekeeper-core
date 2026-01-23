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
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.api.schema.GenericColumn;
import org.pgcodekeeper.core.database.api.schema.ObjectLocation;
import org.pgcodekeeper.core.database.base.parser.AntlrTask;
import org.pgcodekeeper.core.database.base.schema.AbstractDatabase;
import org.pgcodekeeper.core.database.base.schema.AbstractSchema;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.database.base.schema.StatementOverride;
import org.pgcodekeeper.core.loader.ParserListenerMode;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.monitor.NullMonitor;
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
public abstract class AbstractDumpLoader<T extends AbstractDatabase> extends AbstractLoader implements IDumpLoader {

    protected final InputStreamProvider input;
    protected final String inputObjectName;
    protected final IMonitor monitor;
    protected final int monitoringLevel;

    protected ParserListenerMode mode = ParserListenerMode.NORMAL;
    protected Map<AbstractStatement, StatementOverride> overrides;

    protected AbstractDumpLoader(InputStreamProvider input, String inputObjectName,
            ISettings settings, IMonitor monitor, int monitoringLevel) {
        super(settings);
        this.input = input;
        this.inputObjectName = inputObjectName;
        this.monitor = monitor;
        this.monitoringLevel = monitoringLevel;
    }

    protected AbstractDumpLoader(InputStreamProvider input, String inputObjectName, ISettings settings) {
        this(input, inputObjectName, settings, new NullMonitor(), 0);
    }

    protected AbstractDumpLoader(Path inputFile, ISettings settings, IMonitor monitor) {
        this(() -> Files.newInputStream(inputFile), inputFile.toString(), settings, monitor, 1);
    }

    protected AbstractDumpLoader(Path inputFile, ISettings settings) {
        this(() -> Files.newInputStream(inputFile), inputFile.toString(), settings, new NullMonitor(), 0);
    }

    @Override
    public T load() throws IOException, InterruptedException {
        var db = createDatabaseWithSchema();
        IMonitor.checkCancelled(monitor);
        loadWithoutAnalyze(db, antlrTasks);
        finishLoaders();
        return db;
    }

    public T createDatabaseWithSchema() {
        T db = createDatabase();
        AbstractSchema schema = createDefaultSchema();
        db.addSchema(schema);
        ObjectLocation loc = new ObjectLocation.Builder()
                .setObject(new GenericColumn(schema.getName(), DbObjType.SCHEMA))
                .build();
        schema.setLocation(loc);
        db.setDefaultSchema(schema.getName());
        return db;
    }

    public void setMode(ParserListenerMode mode) {
        this.mode = mode;
    }

    public abstract void loadWithoutAnalyze(T db, Queue<AntlrTask<?>> antlrTasks);

    protected abstract T createDatabase();

    protected abstract AbstractSchema createDefaultSchema();

    @Override
    protected void prepare() {
        // no impl
    }
}
