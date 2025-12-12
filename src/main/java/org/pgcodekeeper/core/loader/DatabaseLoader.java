/*******************************************************************************
 * Copyright 2017-2025 TAXTELECOM, LLC
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
package org.pgcodekeeper.core.loader;

import org.pgcodekeeper.core.parsers.antlr.base.AntlrTask;
import org.pgcodekeeper.core.parsers.antlr.base.AntlrTaskManager;
import org.pgcodekeeper.core.database.base.schema.AbstractDatabase;
import org.pgcodekeeper.core.database.ch.schema.ChDatabase;
import org.pgcodekeeper.core.database.ms.schema.MsDatabase;
import org.pgcodekeeper.core.database.pg.schema.PgDatabase;
import org.pgcodekeeper.core.settings.ISettings;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

/**
 * Abstract base class for database schema loaders.
 * Provides common functionality for loading database schemas with ANTLR task management
 * and error collection. Supports loading with or without full analysis.
 */
public abstract class DatabaseLoader {

    protected final List<Object> errors;

    protected final Queue<AntlrTask<?>> antlrTasks = new ArrayDeque<>();
    protected final Queue<DatabaseLoader> launchedLoaders = new ArrayDeque<>();

    /**
     * Loads database schema and performs full analysis.
     *
     * @return the loaded and analyzed database schema
     * @throws IOException          if database loading fails
     * @throws InterruptedException if the loading process is interrupted
     */
    public AbstractDatabase loadAndAnalyze() throws IOException, InterruptedException {
        AbstractDatabase d = load();
        FullAnalyze.fullAnalyze(d, errors);
        return d;
    }

    /**
     * Creates a new database instance based on the database type specified in settings.
     *
     * @param settings configuration settings containing the database type
     * @return new database instance of the appropriate type
     */
    public static AbstractDatabase createDb(ISettings settings) {
        return switch (settings.getDbType()) {
            case CH -> new ChDatabase();
            case MS -> new MsDatabase();
            case PG -> new PgDatabase();
        };
    }

    /**
     * Loads database schema without performing full analysis.
     *
     * @return the loaded database schema
     * @throws IOException          if database loading fails
     * @throws InterruptedException if the loading process is interrupted
     */
    public abstract AbstractDatabase load() throws IOException, InterruptedException;

    protected DatabaseLoader() {
        this(new ArrayList<>());
    }

    protected DatabaseLoader(List<Object> errors) {
        this.errors = errors;
    }

    public List<Object> getErrors() {
        return errors;
    }

    protected void finishLoaders() throws InterruptedException, IOException {
        AntlrTaskManager.finish(antlrTasks);
        DatabaseLoader l;
        while ((l = launchedLoaders.poll()) != null) {
            finishLoader(l);
        }
    }

    protected void finishLoader(DatabaseLoader l) {
        if (errors != null) {
            errors.addAll(l.getErrors());
        }
    }
}
