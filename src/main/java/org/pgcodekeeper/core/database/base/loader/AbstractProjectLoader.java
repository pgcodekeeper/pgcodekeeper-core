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
import org.pgcodekeeper.core.database.api.project.IWorkDirs;
import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.api.schema.IPrivilege;
import org.pgcodekeeper.core.database.api.schema.ITable;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.database.base.schema.StatementOverride;
import org.pgcodekeeper.core.dependencieslist.DependenciesReader;
import org.pgcodekeeper.core.library.LibraryXmlStore;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.settings.DiffSettings;
import org.pgcodekeeper.core.utils.Utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
    public static final String ADDITIONAL_DEPENDENCIES_FILE = ".pgcodekeeperdependencies";

    protected final Path metaPath;
    protected final Map<AbstractStatement, StatementOverride> overrides = new LinkedHashMap<>();
    protected final Queue<AbstractDumpLoader<T>> dumpLoaders = new ArrayDeque<>();
    protected final IWorkDirs workDirs;

    protected boolean isOverrideMode;

    private boolean isLib;
    private final Path dirPath;
    private final Collection<String> libXmls;
    private final Collection<String> libs;
    private final Collection<String> libsWithoutPriv;

    protected AbstractProjectLoader(Path dirPath, DiffSettings diffSettings, IWorkDirs workDirs) {
        this(dirPath, diffSettings, workDirs, Collections.emptyList(), Collections.emptyList(),
                Collections.emptyList(), null);
    }

    protected AbstractProjectLoader(Path dirPath, DiffSettings diffSettings, IWorkDirs workDirs,
                                    Collection<String> libXmls, Collection<String> libs,
                                    Collection<String> libsWithoutPriv, Path metaPath) {
        super(diffSettings, dirPath.getFileName().toString());
        this.dirPath = dirPath;
        this.workDirs = workDirs;
        this.libXmls = libXmls;
        this.libs = libs;
        this.libsWithoutPriv = libsWithoutPriv;
        this.metaPath = metaPath;
    }

    @Override
    public T loadInternal() throws InterruptedException, IOException {
        T db = createDatabase();
        loadStructure(dirPath, db);
        IMonitor.checkCancelled(getMonitor());
        finishLoaders();
        IMonitor.checkCancelled(getMonitor());
        if (!isLib) {
            loadLibraries(db);
            IMonitor.checkCancelled(getMonitor());
            loadOverrides(db);
            IMonitor.checkCancelled(getMonitor());
        }
        return db;
    }

    public void setLib(boolean isLib) {
        this.isLib = isLib;
    }

    protected abstract AbstractDumpLoader<T> createDumpLoader(Path file);

    protected abstract AbstractLibraryLoader<T> createLibraryLoader(T db);

    /**
     * Loads the project structure from the given directory, dispatching to the
     * split-by-schema or flat layout based on {@link IWorkDirs#isSplitBySchema()}.
     *
     * @param dir project root directory
     * @param db  target database to populate
     */
    protected void loadStructure(Path dir, T db) throws InterruptedException, IOException {
        if (workDirs.isSplitBySchema()) {
            loadSplitBySchema(dir, db);
        } else {
            loadFlat(dir, db);
        }
    }

    /**
     * Loads the project using the split-by-schema layout: each schema has its own
     * subdirectory under the schema container, and sub-element types (tables,
     * views, etc.) live inside per-schema directories.
     *
     * @param dir project root directory
     * @param db  target database to populate
     */
    private void loadSplitBySchema(Path dir, T db) throws InterruptedException, IOException {
        var dirMapping = workDirs.getDirMapping();
        var schemaDirName = dirMapping.get(IWorkDirs.SCHEMA_KEY).getDirName();
        List<Path> schemas = listSchemaDirs(dir.resolve(schemaDirName));
        Set<String> loadedDirs = new HashSet<>();

        for (var entry : dirMapping.entrySet()) {
            var typeName = entry.getKey();
            var rule = entry.getValue();
            if (rule.isSubElement()) {
                if (loadedDirs.add(rule.getDirName())) {
                    for (var s : schemas) {
                        loadSubdir(s, rule.getDirName(), db, null);
                    }
                }
            } else if (IWorkDirs.SCHEMA_KEY.equals(typeName)) {
                for (var s : schemas) {
                    loadSubdir(s, db, null);
                }
                afterSchemaLoad(db);
            } else if (loadedDirs.add(rule.getDirName())) {
                loadSubdir(dir, rule.getDirName(), db, null);
            }
        }
    }

    /**
     * Loads the project using the flat layout: all object files sit directly
     * under per-type directories, and the schema name is encoded in the filename
     * rather than in a containing directory.
     *
     * @param dir project root directory
     * @param db  target database to populate
     */
    private void loadFlat(Path dir, T db) throws InterruptedException, IOException {
        Predicate<String> schemaFilter = fileName -> isAllowedSchema(fileName.split("\\.")[0]);
        Set<String> loadedDirs = new HashSet<>();

        for (var entry : workDirs.getDirMapping().entrySet()) {
            var typeName = entry.getKey();
            var rule = entry.getValue();
            if (!loadedDirs.add(rule.getDirName())) {
                continue;
            }
            if (rule.isSubElement()) {
                loadSubdir(dir, rule.getDirName(), db, schemaFilter);
            } else if (IWorkDirs.SCHEMA_KEY.equals(typeName)) {
                loadSubdir(dir, rule.getDirName(), db, schemaFilter);
                afterSchemaLoad(db);
            } else {
                loadSubdir(dir, rule.getDirName(), db, null);
            }
        }
    }

    /**
     * Lists the per-schema subdirectories under the given schema container,
     * filtering out ones excluded by {@link #isAllowedSchema(String)}. Returns
     * an empty list if the container directory does not exist.
     *
     * @param schemaDir container directory holding per-schema subdirectories
     * @return matching schema subdirectories, in directory-listing order
     */
    private List<Path> listSchemaDirs(Path schemaDir) throws IOException {
        if (!Files.isDirectory(schemaDir)) {
            return Collections.emptyList();
        }
        try (Stream<Path> stream = Files.list(schemaDir)) {
            return stream
                    .filter(Files::isDirectory)
                    .filter(t -> isAllowedSchema(t.getFileName().toString()))
                    .toList();
        }
    }

    /**
     * Additional actions after schemas load
     *
     * @param db - current database
     *
     */
    protected void afterSchemaLoad(T db) throws InterruptedException, IOException {
        // do nothing by default
    }

    protected void loadSubdir(Path dir, String sub, T db, Predicate<String> checkFilename)
            throws IOException, InterruptedException {
        Path subDir = dir.resolve(sub);
        if (Files.isDirectory(subDir)) {
            loadSubdir(subDir, db, checkFilename);
        }
    }

    private void loadSubdir(Path subDir, T db, Predicate<String> checkFilename)
            throws IOException, InterruptedException {
        try (Stream<Path> files = Files.list(subDir)
                .filter(f -> filterFile(f, checkFilename))
                .sorted()) {
            for (Path f : Utils.streamIterator(files)) {
                IMonitor.checkCancelled(getMonitor());
                var loader = createDumpLoader(f);
                if (isOverrideMode) {
                    loader.setOverridesMap(overrides);
                } else {
                    loader.setWorkDirs(workDirs);
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

    /**
     * This method loads common settings {@link DiffSettings}, if it's need,
     * before comparing database instances{@link IDatabase}.
     */
    @Override
    public void preLoad() throws IOException {
        if (isPreloaded) {
            return;
        }

        if (diffSettings.getSettings().isDisableAutoLoad()) {
            return;
        }

        // load ignored lists
        Path ignoreFile = dirPath.resolve(IGNORE_FILE);
        if (Files.isRegularFile(ignoreFile)) {
            diffSettings.addIgnoreList(ignoreFile);
        }

        Path ignoreSchemaFile = dirPath.resolve(IGNORE_SCHEMA_FILE);
        if (Files.isRegularFile(ignoreSchemaFile)) {
            diffSettings.addIgnoreSchemaList(ignoreSchemaFile);
        }

        // load additional dependencies
        Path depsPath = dirPath.resolve(ADDITIONAL_DEPENDENCIES_FILE);
        if (Files.isRegularFile(depsPath)) {
            diffSettings.addAdditionalDependencies(DependenciesReader.getDependencies(depsPath));
        }
        isPreloaded = true;
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

        if (!diffSettings.getSettings().isDisableAutoLoad()) {
            // check project libraries
            Path depsFile = dirPath.resolve(LibraryXmlStore.FILE_NAME);
            if (Files.isRegularFile(depsFile)) {
                libraryLoader.loadXml(new LibraryXmlStore(depsFile));
            }
        }
        for (String xml : libXmls) {
            IMonitor.checkCancelled(getMonitor());
            libraryLoader.loadXml(new LibraryXmlStore(Path.of(xml)));
        }
        libraryLoader.loadLibraries(false, libs);
        libraryLoader.loadLibraries(true, libsWithoutPriv);
    }
}
