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

import org.pgcodekeeper.core.database.api.loader.IProjectLoader;
import org.pgcodekeeper.core.database.base.schema.AbstractDatabase;
import org.pgcodekeeper.core.ignorelist.IgnoreSchemaList;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.utils.Utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Locale;
import java.util.Queue;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Base project loader for loading database schemas from project directory structures.
 *
 * @param <T> the type of database this loader produces
 */
public abstract class AbstractProjectLoader<T extends AbstractDatabase> extends AbstractLoader<T>
        implements IProjectLoader {

    protected final Queue<AbstractDumpLoader<T>> dumpLoaders = new ArrayDeque<>();

    private final Path dirPath;
    private final IgnoreSchemaList ignoreSchemaList;

    protected AbstractProjectLoader(Path dirPath, ISettings settings,
                                    IMonitor monitor, IgnoreSchemaList ignoreSchemaList) {
        super(settings, monitor);
        this.dirPath = dirPath;
        this.ignoreSchemaList = ignoreSchemaList;
    }

    @Override
    public T load() throws InterruptedException, IOException {
        T db = createDatabase();
        loadStructure(dirPath, db);
        finishLoaders();
        return db;
    }

    protected abstract T createDatabase();

    protected abstract AbstractDumpLoader<T> createDumpLoader(Path file);

    protected abstract void loadStructure(Path dir, T db) throws InterruptedException, IOException;

    protected void loadSubdir(Path dir, String sub, T db) throws IOException {
        loadSubdir(dir, sub, db, null);
    }

    protected void loadSubdir(Path dir, String sub, T db, Predicate<String> checkFilename)
            throws IOException {
        Path subDir = dir.resolve(sub);
        if (!Files.isDirectory(subDir)) {
            return;
        }
        try (Stream<Path> files = Files.list(subDir)
                .filter(f -> filterFile(f, checkFilename))
                .sorted()) {
            for (Path f : Utils.streamIterator(files)) {
                var loader = createDumpLoader(f);
                loader.loadWithoutAnalyze(db, antlrTasks);
                dumpLoaders.add(loader);
            }
        }
    }

    @Override
    protected void finishLoaders() throws InterruptedException, IOException {
        super.finishLoaders();
        AbstractDumpLoader<T> l;
        while ((l = dumpLoaders.poll()) != null) {
            errors.addAll(l.getErrors());
        }
    }

    protected boolean filterFile(Path f, Predicate<String> checkFilename) {
        String fileName = f.getFileName().toString();
        if (!fileName.toLowerCase(Locale.ROOT).endsWith(".sql") || !Files.isRegularFile(f)) {
            return false;
        }
        return checkFilename == null || checkFilename.test(fileName);
    }

    protected boolean isAllowedSchema(String resourceName) {
        return ignoreSchemaList == null || ignoreSchemaList.getNameStatus(resourceName.split("\\.")[0]);
    }

    @Override
    protected void prepare() {
        // no impl
    }
}
