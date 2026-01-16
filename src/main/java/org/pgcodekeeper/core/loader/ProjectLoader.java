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

import org.pgcodekeeper.core.*;
import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.parser.AntlrTaskManager;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.ignorelist.IgnoreSchemaList;
import org.pgcodekeeper.core.database.ms.schema.MsSchema;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.utils.Utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Database loader for pgCodeKeeper project directory structures.
 * Reads database schemas from organized directory structures containing SQL files
 * for different database types (PostgreSQL, Microsoft SQL Server, ClickHouse).
 * Supports override loading and statement replacement functionality.
 */
public class ProjectLoader extends DatabaseLoader {

    protected ISettings settings;
    private final String dirPath;
    protected final IMonitor monitor;
    private final IgnoreSchemaList ignoreSchemaList;
    protected final Map<AbstractStatement, StatementOverride> overrides = new LinkedHashMap<>();

    protected boolean isOverrideMode;

    /**
     * Creates a new project loader with basic configuration.
     *
     * @param dirPath  path to the project directory
     * @param settings loader settings and configuration
     */
    public ProjectLoader(String dirPath, ISettings settings) {
        this(dirPath, settings, null, new ArrayList<>(), null);
    }

    /**
     * Creates a new project loader with error collection.
     *
     * @param dirPath  path to the project directory
     * @param settings loader settings and configuration
     * @param errors   list to collect loading errors
     */
    public ProjectLoader(String dirPath, ISettings settings, List<Object> errors) {
        this(dirPath, settings, null, errors, null);
    }

    /**
     * Creates a new project loader with full configuration.
     *
     * @param dirPath          path to the project directory
     * @param settings         loader settings and configuration
     * @param monitor          progress monitor for tracking loading progress
     * @param errors           list to collect loading errors
     * @param ignoreSchemaList list of schemas to ignore during loading
     */
    public ProjectLoader(String dirPath, ISettings settings,
                         IMonitor monitor, List<Object> errors, IgnoreSchemaList ignoreSchemaList) {
        super(errors);
        this.dirPath = dirPath;
        this.settings = settings;
        this.monitor = monitor;
        this.ignoreSchemaList = ignoreSchemaList;
    }

    @Override
    public AbstractDatabase load() throws InterruptedException, IOException {
        AbstractDatabase db = createDb(settings);

        Path dir = Paths.get(dirPath);
        loadDbStructure(dir, db);
        return db;
    }

    /**
     * Loads statement overrides from the overrides directory and applies them to the database.
     *
     * @param db the database to apply overrides to
     * @throws InterruptedException if loading is interrupted
     * @throws IOException          if file access fails
     */
    public void loadOverrides(AbstractDatabase db) throws InterruptedException, IOException {
        Path dir = Paths.get(dirPath, WorkDirs.OVERRIDES);
        if (settings.isIgnorePrivileges() || !Files.isDirectory(dir)) {
            return;
        }
        isOverrideMode = true;
        try {
            loadDbStructure(dir, db);
            replaceOverrides();
        } finally {
            isOverrideMode = false;
        }
    }

    private void loadDbStructure(Path dir, AbstractDatabase db) throws InterruptedException, IOException {
        switch (settings.getDbType()) {
            case MS:
                loadMsStructure(dir, db);
                break;
            case PG:
                loadPgStructure(dir, db);
                break;
            case CH:
                loadChStructure(dir, db);
                break;
            default:
                throw new IllegalArgumentException(Messages.DatabaseType_unsupported_type + settings.getDbType());
        }
        finishLoaders();
    }

    private void loadChStructure(Path dir, AbstractDatabase db) throws InterruptedException, IOException {
        for (String dirName : WorkDirs.getDirectoryNames(DatabaseType.CH)) {
            if (WorkDirs.CH_DATABASE.equals(dirName)) {
                loadPgChStructure(dir, db, dirName);
            } else {
                loadSubdir(dir, dirName, db);
            }
        }
    }

    private void loadPgStructure(Path dir, AbstractDatabase db) throws InterruptedException, IOException {
        for (String dirName : WorkDirs.getDirectoryNames(DatabaseType.PG)) {
            if (WorkDirs.PG_SCHEMA.equals(dirName)) {
                loadPgChStructure(dir, db, dirName);
            } else {
                loadSubdir(dir, dirName, db);
            }
        }
    }

    private void loadPgChStructure(Path baseDir, AbstractDatabase db, String commonDir)
            throws InterruptedException, IOException {

        Path schemasCommonDir = baseDir.resolve(commonDir);
        // skip walking SCHEMA folder if it does not exist
        if (!Files.isDirectory(schemasCommonDir)) {
            return;
        }

        // new schemas + content
        // step 2
        // read out schemas names, and work in loop on each
        try (Stream<Path> schemas = Files.list(schemasCommonDir)) {
            for (Path schemaDir : Utils.streamIterator(schemas)) {
                if (Files.isDirectory(schemaDir) && isAllowedSchema(schemaDir.getFileName().toString())) {
                    loadSubdir(schemasCommonDir, schemaDir.getFileName().toString(), db);
                    for (DbObjType dirSub : WorkDirs.getDirLoadOrder()) {
                        loadSubdir(schemaDir, dirSub.name(), db);
                    }
                }
            }
        }
    }

    private void loadMsStructure(Path dir, AbstractDatabase db) throws InterruptedException, IOException {
        Path securityFolder = dir.resolve(WorkDirs.MS_SECURITY);

        loadSubdir(securityFolder, WorkDirs.MS_SCHEMAS, db, this::isAllowedSchema);
        // DBO schema check requires schema loads to finish first
        AntlrTaskManager.finish(antlrTasks);
        addDboSchema(db);

        loadSubdir(securityFolder, WorkDirs.MS_ROLES, db);
        loadSubdir(securityFolder, WorkDirs.MS_USERS, db);

        for (String dirSub : WorkDirs.getDirectoryNames(DatabaseType.MS)) {
            if (WorkDirs.isInMsSchema(dirSub)) {
                // get schema name from file names and filter
                loadSubdir(dir, dirSub, db, this::isAllowedSchema);
                continue;
            }
            loadSubdir(dir, dirSub, db);
        }
    }

    protected void addDboSchema(AbstractDatabase db) {
        if (!db.containsSchema(Consts.DBO)) {
            MsSchema schema = new MsSchema(Consts.DBO);
            ObjectLocation loc = new ObjectLocation.Builder()
                    .setObject(new GenericColumn(Consts.DBO, DbObjType.SCHEMA))
                    .build();

            schema.setLocation(loc);
            db.addSchema(schema);
            db.setDefaultSchema(Consts.DBO);
        }
    }

    private void loadSubdir(Path dir, String sub, AbstractDatabase db) throws InterruptedException, IOException {
        loadSubdir(dir, sub, db, null);
    }

    /**
     * @param checkFilename filter for file names without extensions. Can be null.
     */
    private void loadSubdir(Path dir, String sub, AbstractDatabase db, Predicate<String> checkFilename)
            throws InterruptedException, IOException {
        Path subDir = dir.resolve(sub);
        if (!Files.isDirectory(subDir)) {
            return;
        }
        try (Stream<Path> files = Files.list(subDir)
                .filter(f -> filterFile(f, checkFilename))
                .sorted()) {
            for (Path f : Utils.streamIterator(files)) {
                PgDumpLoader loader = new PgDumpLoader(f, settings, monitor);
                if (isOverrideMode) {
                    loader.setOverridesMap(overrides);
                }
                loader.loadDatabase(db, antlrTasks);
                launchedLoaders.add(loader);
            }
        }
    }

    private boolean filterFile(Path f, Predicate<String> checkFilename) {
        String fileName = f.getFileName().toString();
        if (!fileName.toLowerCase(Locale.ROOT).endsWith(".sql") || !Files.isRegularFile(f)) {
            return false;
        }
        return checkFilename == null || checkFilename.test(fileName);
    }

    protected void replaceOverrides() {
        Iterator<Entry<AbstractStatement, StatementOverride>> iterator = overrides.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<AbstractStatement, StatementOverride> entry = iterator.next();
            iterator.remove();

            AbstractStatement st = entry.getKey();
            StatementOverride override = entry.getValue();
            if (override.getOwner() != null) {
                st.setOwner(override.getOwner());
            }

            if (!override.getPrivileges().isEmpty()) {
                st.clearPrivileges();
                if (st.getStatementType() == DbObjType.TABLE) {
                    for (AbstractColumn col : ((AbstractTable) st).getColumns()) {
                        col.clearPrivileges();
                    }
                }
                for (IPrivilege privilege : override.getPrivileges()) {
                    st.addPrivilege(privilege);
                }
            }
        }
    }

    protected boolean isAllowedSchema(String resourceName) {
        return ignoreSchemaList == null || ignoreSchemaList.getNameStatus(resourceName.split("\\.")[0]);
    }
}
