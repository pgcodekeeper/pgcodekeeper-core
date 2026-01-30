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
import org.pgcodekeeper.core.database.base.parser.*;
import org.pgcodekeeper.core.database.base.schema.AbstractDatabase;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.settings.ISettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Base database loader
 */
public abstract class AbstractLoader<T extends AbstractDatabase> implements ILoader {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractLoader.class);

    protected final List<Object> errors = new ArrayList<>();
    protected final Queue<AntlrTask<?>> antlrTasks = new ArrayDeque<>();
    protected final Queue<AbstractLoader<T>> launchedLoaders = new ArrayDeque<>();

    protected final ISettings settings;
    protected final IMonitor monitor;

    protected String currentOperation;
    protected int version;

    protected AbstractLoader(ISettings settings, IMonitor monitor) {
        this.settings = settings;
        this.monitor = monitor;
        prepare();
    }

    @Override
    public abstract T load() throws IOException, InterruptedException;

    public List<Object> getErrors() {
        return errors;
    }

    public void addError(Object error) {
        errors.add(error);
    }

    protected void prepare() {
        // subclasses will override if needed
    }

    protected void finishLoaders() throws InterruptedException, IOException {
        AntlrTaskManager.finish(antlrTasks);
        AbstractLoader<T> l;
        while ((l = launchedLoaders.poll()) != null) {
            finishLoader(l);
        }
    }

    protected void finishLoader(AbstractLoader<T> l) {
        errors.addAll(l.getErrors());
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
        return settings;
    }
}
