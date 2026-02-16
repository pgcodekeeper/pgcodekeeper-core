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

import org.pgcodekeeper.core.database.api.loader.ILoader;
import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.base.parser.AntlrTask;
import org.pgcodekeeper.core.database.base.parser.AntlrTaskManager;
import org.pgcodekeeper.core.database.base.parser.FullAnalyze;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.settings.DiffSettings;
import org.pgcodekeeper.core.settings.ISettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Queue;

/**
 * Base database loader
 */
public abstract class AbstractLoader<T extends IDatabase> implements ILoader {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractLoader.class);

    protected final Queue<AntlrTask<?>> antlrTasks = new ArrayDeque<>();

    protected final DiffSettings diffSettings;

    protected String currentOperation;
    protected int version;

    protected AbstractLoader(DiffSettings diffSettings) {
        this.diffSettings = diffSettings;
    }

    @Override
    public abstract T load() throws IOException, InterruptedException;

    public List<Object> getErrors() {
        return Collections.unmodifiableList(diffSettings.getErrors());
    }

    public void addError(Object error) {
        diffSettings.addError(error);
    }

    /**
     * Loads the database and performs full expression analysis.
     *
     * @return fully loaded and analyzed database
     * @throws IOException          if database loading fails
     * @throws InterruptedException if the loading process is interrupted
     */
    public T loadAndAnalyze() throws IOException, InterruptedException {
        T db = load();
        FullAnalyze.fullAnalyze(db, diffSettings.getErrors());
        return db;
    }

    protected void finishLoaders() throws InterruptedException, IOException {
        AntlrTaskManager.finish(antlrTasks);
    }

    protected void debug(String message, Object... args) {
        if (LOG.isDebugEnabled()) {
            var msg = message.formatted(args);
            LOG.debug(msg);
        }
    }

    protected void info(String message, Object... args) {
        var msg = message.formatted(args);
        LOG.info(msg);
    }

    /**
     * Creates a new database instance.
     *
     * @return new database instance of the appropriate type
     */
    protected abstract T createDatabase();

    @Override
    public ISettings getSettings() {
        return diffSettings.getSettings();
    }

    public IMonitor getMonitor() {
        return diffSettings.getMonitor();
    }

    public boolean isAllowedSchema(String schemaName) {
        return diffSettings.isAllowedSchema(schemaName);
    }
}
