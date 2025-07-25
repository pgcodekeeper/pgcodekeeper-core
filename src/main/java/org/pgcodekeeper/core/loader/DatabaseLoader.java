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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.parsers.antlr.AntlrTask;
import org.pgcodekeeper.core.parsers.antlr.AntlrTaskManager;
import org.pgcodekeeper.core.schema.AbstractDatabase;
import org.pgcodekeeper.core.schema.ch.ChDatabase;
import org.pgcodekeeper.core.schema.ms.MsDatabase;
import org.pgcodekeeper.core.schema.pg.PgDatabase;
import org.pgcodekeeper.core.settings.ISettings;

public abstract class DatabaseLoader {

    protected final List<Object> errors;

    protected final Queue<AntlrTask<?>> antlrTasks = new ArrayDeque<>();
    protected final Queue<DatabaseLoader> launchedLoaders = new ArrayDeque<>();

    /**
     * Loads database schema with analyze.
     *
     * @return database schema
     */
    public AbstractDatabase loadAndAnalyze() throws IOException, InterruptedException {
        AbstractDatabase d = load();
        FullAnalyze.fullAnalyze(d, errors);
        return d;
    }

    public static AbstractDatabase createDb(ISettings settings) {
        return switch (settings.getDbType()) {
        case CH -> new ChDatabase();
        case MS -> new MsDatabase();
        case PG -> new PgDatabase();
        default -> throw new IllegalArgumentException(Messages.DatabaseType_unsupported_type + settings.getDbType());
        };
    }

    /**
     * Loads database schema without analyze.
     *
     * @return database schema
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
