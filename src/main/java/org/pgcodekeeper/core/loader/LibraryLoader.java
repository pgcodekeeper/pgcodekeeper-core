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
package org.pgcodekeeper.core.loader;

import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.utils.Utils;
import org.pgcodekeeper.core.library.PgLibrary;
import org.pgcodekeeper.core.library.PgLibrarySource;
import org.pgcodekeeper.core.database.base.schema.AbstractDatabase;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.utils.FileUtils;
import org.pgcodekeeper.core.xmlstore.DependenciesXmlStore;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

/**
 * Database loader for external libraries and dependencies.
 * Loads database schemas from library sources including JAR files, directories, and XML dependency definitions.
 * Supports nested library loading and prevents circular dependencies.
 */
public final class LibraryLoader extends DatabaseLoader {

    private final AbstractDatabase database;
    private final Path metaPath;
    private final Set<String> loadedLibs;

    private boolean loadNested;

    /**
     * Creates a new library loader with empty loaded paths set.
     *
     * @param database the target database to load libraries into
     * @param metaPath path to metadata directory
     * @param errors   list to collect loading errors
     */
    public LibraryLoader(AbstractDatabase database, Path metaPath, List<Object> errors) {
        this(database, metaPath, errors, new HashSet<>());
    }

    /**
     * Creates a new library loader with specified loaded paths set.
     *
     * @param database    the target database to load libraries into
     * @param metaPath    path to metadata directory
     * @param errors      list to collect loading errors
     * @param loadedPaths set of already loaded library paths to prevent circular dependencies
     */
    public LibraryLoader(AbstractDatabase database, Path metaPath, List<Object> errors, Set<String> loadedPaths) {
        super(errors);
        this.database = database;
        this.metaPath = metaPath;
        this.loadedLibs = loadedPaths;
    }

    /**
     * Not supported operation for library loader.
     *
     * @throws IllegalStateException always, as this operation is not supported
     */
    @Override
    public AbstractDatabase load() {
        throw new IllegalStateException("Unsupported operation for LibraryLoader");
    }

    /**
     * Loads libraries from the specified collection of paths.
     *
     * @param settings           loader settings and configuration
     * @param isIgnorePrivileges whether to ignore privileges during loading
     * @param paths              collection of library paths to load
     * @throws InterruptedException if loading is interrupted
     * @throws IOException          if library loading fails
     */
    public void loadLibraries(ISettings settings, boolean isIgnorePrivileges,
                              Collection<String> paths) throws InterruptedException, IOException {
        for (String path : paths) {
            database.addLib(getLibrary(path, settings, isIgnorePrivileges), path, null);
        }
    }

    /**
     * Loads libraries from XML dependency store configuration.
     *
     * @param xmlStore the XML store containing dependency definitions
     * @param settings loader settings and configuration
     * @throws InterruptedException if loading is interrupted
     * @throws IOException          if XML loading fails
     */
    public void loadXml(DependenciesXmlStore xmlStore, ISettings settings)
            throws InterruptedException, IOException {
        List<PgLibrary> xmlLibs = xmlStore.readObjects();
        Path xmlPath = xmlStore.getXmlFile();
        boolean oldLoadNested = loadNested;
        try {
            loadNested = xmlStore.readLoadNestedFlag();
            for (PgLibrary lib : xmlLibs) {
                String path = lib.path();
                AbstractDatabase l = getLibrary(path, settings, lib.isIgnorePrivileges(), xmlPath);
                database.addLib(l, path, lib.owner());
            }
        } finally {
            loadNested = oldLoadNested;
        }
    }

    private AbstractDatabase getLibrary(String path, ISettings settings, boolean isIgnorePrivileges)
            throws InterruptedException, IOException {
        return getLibrary(path, settings, isIgnorePrivileges, null);
    }

    private AbstractDatabase getLibrary(String path, ISettings settings, boolean isIgnorePrivileges, Path xmlPath)
            throws InterruptedException, IOException {
        if (!loadedLibs.add(path)) {
            return createDb(settings);
        }

        ISettings copySettings;
        if (!settings.isIgnorePrivileges()) {
            copySettings = settings.copy();
            copySettings.setIgnorePrivileges(isIgnorePrivileges);
        } else {
            copySettings = settings;
        }

        switch (PgLibrarySource.getSource(path)) {
            case JDBC:
//                return loadJdbc(copySettings, path);
            case URL:
                try {
                    return loadURI(new URI(path), copySettings, isIgnorePrivileges);
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
            throw new IOException("Error while read library : %s - File not found".formatted(path));
        }

        if (Files.isDirectory(p)) {
            if (Files.exists(p.resolve(Consts.FILENAME_WORKING_DIR_MARKER))) {
                AbstractDatabase db = new ProjectLoader(p.toString(), copySettings, errors).load();

                if (loadNested) {
                    new LibraryLoader(db, metaPath, errors, loadedLibs).loadXml(
                            new DependenciesXmlStore(p.resolve(DependenciesXmlStore.FILE_NAME)), copySettings);
                }

                return db;
            }

            AbstractDatabase db = createDb(copySettings);
            readStatementsFromDirectory(p, db, copySettings);
            finishLoaders();
            return db;
        }

        if (FileUtils.isZipFile(p)) {
            return loadZip(p, copySettings, isIgnorePrivileges);
        }

        AbstractDatabase db = createDb(copySettings);
        PgDumpLoader loader = new PgDumpLoader(p, copySettings);
        loader.loadAsync(db, antlrTasks);
        launchedLoaders.add(loader);
        finishLoaders();
        return db;
    }

    private AbstractDatabase loadZip(Path path, ISettings settings, boolean isIgnorePrivileges)
            throws InterruptedException, IOException {
        Path dir = FileUtils.getUnzippedFilePath(metaPath, path);
        return getLibrary(FileUtils.unzip(path, dir), settings, isIgnorePrivileges);
    }

//    private AbstractDatabase loadJdbc(ISettings settings, String path) throws IOException, InterruptedException {
//        DatabaseLoader loader = LoaderFactory.createJdbcLoader(settings, path, null);
//        AbstractDatabase db;
//        try {
//            db = loader.load();
//        } finally {
//            errors.addAll(loader.getErrors());
//        }
//
//        return db;
//    }

    private AbstractDatabase loadURI(URI uri, ISettings settings, boolean isIgnorePrivileges)
            throws InterruptedException, IOException {
        String path = uri.getPath();
        String fileName = FileUtils.getValidFilename(Paths.get(path).getFileName().toString());
        String name = fileName + '_' + Utils.md5(path).substring(0, 10);

        Path dir = metaPath.resolve(name);

        FileUtils.loadURI(uri, fileName, dir);

        return getLibrary(dir.toString(), settings, isIgnorePrivileges);
    }

    private void readStatementsFromDirectory(Path f, AbstractDatabase db, ISettings settings)
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

    private void readStatementsFromFile(Path sub, AbstractDatabase db, ISettings settings)
            throws InterruptedException, IOException {
        String filePath = sub.toString();
        if (filePath.endsWith(".zip")) {
            db.addLib(getLibrary(filePath, settings, settings.isIgnorePrivileges()), null, null);
        } else if (filePath.endsWith(".sql")) {
            PgDumpLoader loader = new PgDumpLoader(sub, settings);
            loader.loadDatabase(db, antlrTasks);
            launchedLoaders.add(loader);
        }
    }
}
