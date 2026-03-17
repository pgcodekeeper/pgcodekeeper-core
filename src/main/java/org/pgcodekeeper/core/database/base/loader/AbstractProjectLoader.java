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

import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.database.api.loader.IProjectLoader;
import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.api.schema.IPrivilege;
import org.pgcodekeeper.core.database.api.schema.ITable;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.database.base.schema.StatementOverride;
import org.pgcodekeeper.core.library.LibraryXmlStore;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.settings.DiffSettings;
import org.pgcodekeeper.core.utils.Utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Base project loader for loading database schemas from project directory structures.
 *
 * @param <T> the type of database this loader produces
 */
public abstract class AbstractProjectLoader<T extends IDatabase> extends AbstractLoader<T>
        implements IProjectLoader {

    public static final String IGNORE_FILE = ".pgcodekeeperignore";
    public static final String IGNORE_SCHEMA_FILE = ".pgcodekeeperignoreschema";
    public static final String OVERRIDES_DIR = "OVERRIDES";

    protected final Path metaPath;
    protected final Map<AbstractStatement, StatementOverride> overrides = new LinkedHashMap<>();
    protected final Queue<AbstractDumpLoader<T>> dumpLoaders = new ArrayDeque<>();

    protected boolean isOverrideMode;

    private boolean isLib;
    private final Path dirPath;
    private final Collection<String> libXmls;
    private final Collection<String> libs;
    private final Collection<String> libsWithoutPriv;

    protected AbstractProjectLoader(Path dirPath, DiffSettings diffSettings) {
        this(dirPath, diffSettings, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
                null);
    }

    protected AbstractProjectLoader(Path dirPath, DiffSettings diffSettings, Collection<String> libXmls,
                                    Collection<String> libs, Collection<String> libsWithoutPriv, Path metaPath) {
        super(diffSettings, dirPath.getFileName().toString());
        this.dirPath = dirPath;
        this.libXmls = libXmls;
        this.libs = libs;
        this.libsWithoutPriv = libsWithoutPriv;
        this.metaPath = metaPath;
        readIgnoreLists();
    }

    @Override
    public T loadInternal() throws InterruptedException, IOException {
        T db = createDatabase();
        loadStructure(dirPath, db);
        IMonitor.checkCancelled(getMonitor());
        finishLoaders();
        IMonitor.checkCancelled(getMonitor());
        loadLibraries(db);
        IMonitor.checkCancelled(getMonitor());
        loadOverrides(db);
        return db;
    }

    public void setLib(boolean isLib) {
        this.isLib = isLib;
    }

    @Override
    protected abstract T createDatabase();

    protected abstract AbstractDumpLoader<T> createDumpLoader(Path file);

    protected abstract AbstractLibraryLoader<T> createLibraryLoader(T db);

    protected abstract void loadStructure(Path dir, T db) throws InterruptedException, IOException;

    protected void loadSubdir(Path dir, String sub, T db) throws IOException, InterruptedException {
        loadSubdir(dir, sub, db, null);
    }

    protected void loadSubdir(Path dir, String sub, T db, Predicate<String> checkFilename)
            throws IOException, InterruptedException {
        Path subDir = dir.resolve(sub);
        if (!Files.isDirectory(subDir)) {
            return;
        }
        try (Stream<Path> files = Files.list(subDir)
                .filter(f -> filterFile(f, checkFilename))
                .sorted()) {
            for (Path f : Utils.streamIterator(files)) {
                IMonitor.checkCancelled(getMonitor());
                var loader = createDumpLoader(f);
                if (isOverrideMode) {
                    loader.setOverridesMap(overrides);
                }
                loader.loadWithoutAnalyze(db, antlrTasks);
                dumpLoaders.add(loader);
            }
        }
    }

    @Override
    protected void finishLoaders() throws InterruptedException, IOException {
        super.finishLoaders();
        dumpLoaders.clear();
    }

    protected boolean filterFile(Path f, Predicate<String> checkFilename) {
        String fileName = f.getFileName().toString();
        if (!fileName.toLowerCase(Locale.ROOT).endsWith(Consts.SQL_POSTFIX) || !Files.isRegularFile(f)) {
            return false;
        }
        return checkFilename == null || checkFilename.test(fileName);
    }

    private void loadOverrides(T db) throws IOException, InterruptedException {
        Path overridesDir = dirPath.resolve(OVERRIDES_DIR);
        if (getSettings().isIgnorePrivileges() || !Files.isDirectory(overridesDir)) {
            return;
        }

        isOverrideMode = true;
        try {
            loadStructure(overridesDir, db);
            finishLoaders();
            IMonitor.checkCancelled(getMonitor());
            replaceOverrides();
        } finally {
            isOverrideMode = false;
        }
    }

    private void replaceOverrides() {
        Iterator<Map.Entry<AbstractStatement, StatementOverride>> iterator = overrides.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<AbstractStatement, StatementOverride> entry = iterator.next();
            iterator.remove();

            AbstractStatement st = entry.getKey();
            StatementOverride override = entry.getValue();
            if (override.getOwner() != null) {
                st.setOwner(override.getOwner());
            }

            if (!override.getPrivileges().isEmpty()) {
                replacePrivileges(st, override);
            }
        }
    }

    private void replacePrivileges(AbstractStatement st, StatementOverride override) {
        st.clearPrivileges();
        if (st instanceof ITable table) {
            for (var col : table.getColumns()) {
                col.clearPrivileges();
            }
        }
        for (IPrivilege privilege : override.getPrivileges()) {
            st.addPrivilege(privilege);
        }
    }

    private void loadLibraries(T db) throws IOException, InterruptedException {
        var libraryLoader = createLibraryLoader(db);

        if (!isLib && !diffSettings.getSettings().isDisableAutoLoad()) {
            // check project libraries
            Path depsFile = dirPath.resolve(LibraryXmlStore.FILE_NAME);
            if (Files.isRegularFile(depsFile)) {
                libraryLoader.loadXml(new LibraryXmlStore(depsFile));
            }
        }
        for (String xml : libXmls) {
            IMonitor.checkCancelled(getMonitor());
            libraryLoader.loadXml(new LibraryXmlStore(Paths.get(xml)));
        }
        libraryLoader.loadLibraries(false, libs);
        libraryLoader.loadLibraries(true, libsWithoutPriv);
    }

    private void readIgnoreLists() {
        if (diffSettings.getSettings().isDisableAutoLoad()) {
            return;
        }

        try {
            Path ignoreFile = dirPath.resolve(IGNORE_FILE);
            if (Files.isRegularFile(ignoreFile)) {
                diffSettings.addIgnoreList(ignoreFile);
            }

            Path ignoreSchemaFile = dirPath.resolve(IGNORE_SCHEMA_FILE);
            if (Files.isRegularFile(ignoreSchemaFile)) {
                diffSettings.addIgnoreSchemaList(ignoreSchemaFile);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to read ignore lists", e);
        }
    }
}
