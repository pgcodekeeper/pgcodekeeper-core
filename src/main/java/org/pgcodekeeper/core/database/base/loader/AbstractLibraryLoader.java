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
import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.library.Library;
import org.pgcodekeeper.core.library.LibrarySource;
import org.pgcodekeeper.core.library.LibraryXmlStore;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.utils.FileUtils;
import org.pgcodekeeper.core.utils.Utils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Database loader for external library dependencies.
 * Loads database schemas from dependency sources including JAR files, directories, and XML dependency definitions.
 * Supports nested dependency loading and prevents circular dependencies.
 */
public abstract class AbstractLibraryLoader<T extends IDatabase> extends AbstractLoader<T> {

    protected final T database;
    protected final Path metaPath;
    protected final Set<String> loadedPaths;

    protected boolean loadNested;

    protected AbstractLibraryLoader(T database, Path metaPath, Set<String> loadedPaths,
            ISettings settings, IMonitor monitor) {
        super(settings, monitor);
        this.database = database;
        this.metaPath = metaPath;
        this.loadedPaths = loadedPaths;
    }

    /**
     * Not supported operation for library dependency loader.
     *
     * @throws UnsupportedOperationException always, as this operation is not supported
     */
    @Override
    public T load() {
        throw new UnsupportedOperationException();
    }

    /**
     * Loads libraries from the specified collection of paths.
     *
     * @param isIgnorePrivileges whether to ignore privileges during loading
     * @param paths              collection of library paths to load
     * @throws InterruptedException if loading is interrupted
     * @throws IOException          if library loading fails
     */
    public void loadLibraries(boolean isIgnorePrivileges, Collection<String> paths)
            throws InterruptedException, IOException {
        for (String path : paths) {
            database.addLib(getLibraryDependency(path, isIgnorePrivileges), path, null);
        }
    }

    /**
     * Loads library dependencies from XML store configuration.
     *
     * @param xmlStore the XML store containing dependency definitions
     * @throws InterruptedException if loading is interrupted
     * @throws IOException          if XML loading fails
     */
    public void loadXml(LibraryXmlStore xmlStore)
            throws InterruptedException, IOException {
        List<Library> libs = xmlStore.readObjects();
        Path xmlPath = xmlStore.getXmlFile();
        boolean oldLoadNested = loadNested;
        try {
            loadNested = xmlStore.readLoadNestedFlag();
            for (Library lib : libs) {
                String path = lib.path();
                T db = getLibraryDependency(path, lib.isIgnorePrivileges(), xmlPath);
                database.addLib(db, path, lib.owner());
            }
        } finally {
            loadNested = oldLoadNested;
        }
    }

    private T getLibraryDependency(String path, boolean isIgnorePrivileges)
            throws InterruptedException, IOException {
        return getLibraryDependency(path, isIgnorePrivileges, null);
    }

    private T getLibraryDependency(String path, boolean isIgnorePrivileges, Path xmlPath)
            throws InterruptedException, IOException {
        if (!loadedPaths.add(path)) {
            return createDatabase();
        }

        switch (LibrarySource.getSource(path)) {
            case JDBC:
                return loadJdbc(path, isIgnorePrivileges);
            case URL:
                try {
                    return loadURI(new URI(path), isIgnorePrivileges);
                } catch (URISyntaxException ex) {
                    // shouldn't happen, already checked by getSource
                    // not URI, try to folder or file
                    break;
                }
            case LOCAL:
                // continue below
        }
        Path p = Paths.get(path);
        if (!p.isAbsolute() && xmlPath != null) {
            p = xmlPath.resolveSibling(p).normalize();
        }

        if (!Files.exists(p)) {
            throw new IOException("Error while read library dependency : %s - File not found".formatted(path));
        }

        if (Files.isDirectory(p)) {
            return loadDirectory(p, isIgnorePrivileges);
        }

        if (FileUtils.isZipFile(p)) {
            return loadZip(p, isIgnorePrivileges);
        }

        return loadDump(p, isIgnorePrivileges);
    }

    /**
     * Loads the database from a directory.
     *
     * @param path path to directory
     * @param isIgnorePrivileges library setting for ignoring privileges
     * @return loaded database
     */
    private T loadDirectory(Path path, boolean isIgnorePrivileges) throws IOException, InterruptedException {
        ISettings copySettings = getSettingsCopy(isIgnorePrivileges);

        // project
        if (Files.exists(path.resolve(Consts.FILENAME_WORKING_DIR_MARKER))) {
            T db = getProjectLoader(path, copySettings).load();

            if (loadNested) {
                getCopy(db).loadXml(new LibraryXmlStore(path.resolve(LibraryXmlStore.FILE_NAME)));
            }

            return db;
        }

        // simple directory
        T db = createDatabase();
        readStatementsFromDirectory(path, db, copySettings);
        finishLoaders();
        return db;
    }

    /**
     * Creates a copy of the settings with the isIgnorePrivileges parameter overridden.
     *
     * @param isIgnorePrivileges new value
     * @return new settings
     */
    private ISettings getSettingsCopy(boolean isIgnorePrivileges) {
        if (settings.isIgnorePrivileges()) {
            return settings;
        }

        ISettings copySettings = settings.copy();
        copySettings.setIgnorePrivileges(isIgnorePrivileges);
        return copySettings;
    }

    private T loadZip(Path path, boolean isIgnorePrivileges)
            throws InterruptedException, IOException {
        Path dir = FileUtils.getUnzippedFilePath(metaPath, path);
        return getLibraryDependency(FileUtils.unzip(path, dir), isIgnorePrivileges);
    }

    /**
     * Loads the database from jdbc.
     *
     * @param url jdbc url
     * @param isIgnorePrivileges library setting for ignoring privileges
     * @return loaded database
     */
    private T loadJdbc(String url, boolean isIgnorePrivileges) throws IOException, InterruptedException {
        var loader = createJdbcLoader(url, getSettingsCopy(isIgnorePrivileges));
        T db = loader.load();
        errors.addAll(loader.getErrors());
        return db;
    }

    /**
     * Loads the database from a single dump.
     *
     * @param path path to file
     * @param isIgnorePrivileges library setting for ignoring privileges
     * @return loaded database
     */
    private T loadDump(Path path, boolean isIgnorePrivileges) throws IOException, InterruptedException {
        var loader = getDumpLoader(path, getSettingsCopy(isIgnorePrivileges));
        T db = loader.load();
        errors.addAll(loader.getErrors());
        return db;
    }

    /**
     * Loads the database from a URI.
     *
     * @param uri uri
     * @param isIgnorePrivileges library setting for ignoring privileges
     * @return loaded database
     */
    private T loadURI(URI uri, boolean isIgnorePrivileges)
            throws InterruptedException, IOException {
        String path = uri.getPath();
        String fileName = FileUtils.getValidFilename(Paths.get(path).getFileName().toString());
        String name = fileName + '_' + Utils.md5(path).substring(0, 10);

        Path dir = metaPath.resolve(name);

        FileUtils.loadURI(uri, fileName, dir);

        return getLibraryDependency(dir.toString(), isIgnorePrivileges);
    }

    private void readStatementsFromDirectory(Path f, T db, ISettings settings)
            throws IOException, InterruptedException {
        try (Stream<Path> stream = Files.list(f)) {
            List<Path> dirs = new ArrayList<>();
            for (Path sub : (Iterable<Path>) stream::iterator) {
                if (Files.isDirectory(sub)) {
                    dirs.add(sub);
                } else {
                    readStatementsFromFile(sub, db, settings);
                }
            }

            for (Path sub : dirs) {
                readStatementsFromDirectory(sub, db, settings);
            }
        }
    }

    private void readStatementsFromFile(Path sub, T db, ISettings settings)
            throws InterruptedException, IOException {
        String filePath = sub.toString();
        if (filePath.endsWith(".zip")) {
            db.addLib(getLibraryDependency(filePath, settings.isIgnorePrivileges()), null, null);
        } else if (filePath.endsWith(".sql")) {
            var loader = getDumpLoader(sub, settings);
            loader.loadWithoutAnalyze(db, antlrTasks);
            launchedLoaders.add(loader);
        }
    }

    /**
     * Returns a dump loader specific to the database type.
     *
     * @param path file path
     * @param settings loader settings
     * @return loader
     */
    protected abstract AbstractDumpLoader<T> getDumpLoader(Path path, ISettings settings);

    /**
     * Returns a jdbc loader specific to the database type.
     *
     * @param url jdbc url
     * @param settings loader settings
     * @return loader
     */
    protected abstract AbstractJdbcLoader<T> createJdbcLoader(String url, ISettings settings);

    /**
     * Returns a project loader specific to the database type.
     *
     * @param path project path
     * @param settings loader settings
     * @return loader
     */
    protected abstract AbstractProjectLoader<T> getProjectLoader(Path path, ISettings settings);

    /**
     * Creates a copy of the loader to load nested libraries.
     *
     * @param db - current database
     * @return library loader copy
     */
    protected abstract AbstractLibraryLoader<T> getCopy(T db);
}
