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
package org.pgcodekeeper.core.model.exporter;

import org.pgcodekeeper.core.*;
import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.model.difftree.TreeElement;
import org.pgcodekeeper.core.database.base.schema.AbstractDatabase;
import org.pgcodekeeper.core.settings.CoreSettings;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.utils.FileUtils;
import org.pgcodekeeper.core.utils.UnixPrintWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Exports database model as directory tree with SQL files containing object definitions.
 * Supports full, partial, and project exports with configurable encoding and database type support.
 * <p>
 * For historical reasons we expect a filtered user-selection-only list in {@link #exportPartial()} but we use the new
 * API {@link TreeElement#isSelected()} for selection checks instead of calling {@link Collection#contains(Object)} for
 * performance reasons.
 *
 * @author Alexander Levsha
 */
public class ModelExporter {

    private static final Logger LOG = LoggerFactory.getLogger(ModelExporter.class);

    public static final String GROUP_DELIMITER =
            "\n\n--------------------------------------------------------------------------------\n\n"; //$NON-NLS-1$

    /**
     * Objects of the export directory
     */
    protected final Path outDir;

    /**
     * Database to export
     */
    protected final AbstractDatabase newDb;

    /**
     * Old state db to fetch filenames from
     */
    protected final AbstractDatabase oldDb;

    /**
     * SQL files encoding
     */
    private final String sqlEncoding;

    /**
     * Objects that we need to operate on
     */
    protected final Collection<TreeElement> changeList;

    /**
     * Database type for defining directory structure
     */
    private final DatabaseType databaseType;

    protected final ISettings settings;

    /**
     * Creates a new ModelExporter for full database export.
     *
     * @param outDir       output directory, should be empty or not exist
     * @param db           database to export
     * @param databaseType database type for directory structure
     * @param sqlEncoding  SQL file encoding
     * @param settings     export settings
     */
    public ModelExporter(Path outDir, AbstractDatabase db, DatabaseType databaseType, String sqlEncoding,
                         ISettings settings) {
        this(outDir, db, null, databaseType, null, sqlEncoding, settings);
    }

    /**
     * Creates a new ModelExporter for partial or project export.
     *
     * @param outDir         output directory
     * @param newDb          new database schema
     * @param oldDb          old database schema, can be null for project export
     * @param databaseType   database type for directory structure
     * @param changedObjects collection of changed objects
     * @param sqlEncoding    SQL file encoding
     * @param settings       export settings
     */
    public ModelExporter(Path outDir, AbstractDatabase newDb, AbstractDatabase oldDb,
                         DatabaseType databaseType, Collection<TreeElement> changedObjects, String sqlEncoding, ISettings settings) {
        this.outDir = outDir;
        this.newDb = newDb;
        this.oldDb = oldDb;
        this.sqlEncoding = sqlEncoding;
        this.changeList = changedObjects;
        this.databaseType = databaseType;

        // we should create new settings to get correct script in project files
        var copySettings = new CoreSettings();
        copySettings.setDbType(settings.getDbType());
        this.settings = copySettings;
    }

    /**
     * Exports the complete database schema to directory structure.
     * Creates output directory and exports all database objects as SQL files.
     *
     * @throws IOException if export operation fails
     */
    public void exportFull() throws IOException {
        createOutDir();

        Map<Path, StringBuilder> dumps = new HashMap<>();
        newDb.getDescendants().sorted(ExportTableOrder.INSTANCE).forEach(st -> dumpStatement(st, dumps));

        writeDumps(dumps);
    }

    private void createOutDir() throws IOException {
        LOG.info(Messages.ModelExporter_log_create_dirs);
        if (Files.exists(outDir)) {
            if (!Files.isDirectory(outDir)) {
                var msg = Messages.ModelExporter_log_create_dir_err_no_dir.formatted(outDir);
                LOG.error(msg);
                throw new NotDirectoryException(outDir.toString());
            }

            for (String subdirName : WorkDirs.getDirectoryNames(databaseType)) {
                if (Files.exists(outDir.resolve(subdirName))) {
                    String msg = Messages.ModelExporter_log_create_dir_err_contains_dir.formatted(subdirName);
                    LOG.error(msg);
                    throw new DirectoryException(msg);
                }
            }
        } else {
            Files.createDirectories(outDir);
        }
    }

    /**
     * Exports only changed objects based on comparison between old and new schemas.
     * Handles object additions, deletions, and modifications.
     *
     * @throws IOException           if export operation fails
     * @throws PgCodekeeperException if old database is null or directory issues occur
     */
    public void exportPartial() throws IOException, PgCodekeeperException {
        if (oldDb == null) {
            String msg = Messages.ModelExporter_log_old_database_not_null;
            LOG.error(msg);
            throw new PgCodekeeperException(msg);
        }
        if (Files.notExists(outDir) || !Files.isDirectory(outDir)) {
            throw new DirectoryException(Messages.ModelExporter_log_output_dir_no_exist_err.formatted(
                    outDir.toAbsolutePath()));
        }

        List<IStatement> list = oldDb.getDescendants().collect(Collectors.toList());
        Set<Path> paths = new HashSet<>();

        for (TreeElement el : changeList) {
            if (el.getType() == DbObjType.DATABASE) {
                continue;
            }
            switch (el.getSide()) {
                case LEFT:
                    var stInOld = el.getStatement(oldDb);
                    list.remove(stInOld);
                    for (var child : Utils.streamIterator(stInOld.getChildren())) {
                        list.remove(child);
                        deleteStatementIfExists(child);
                    }
                    paths.add(getRelativeFilePath(stInOld));
                    deleteStatementIfExists(stInOld);
                    break;
                case RIGHT:
                    var stInNew = el.getStatement(newDb);
                    list.add(stInNew);
                    paths.add(getRelativeFilePath(stInNew));
                    deleteStatementIfExists(stInNew);
                    break;
                case BOTH:
                    stInNew = el.getStatement(newDb);
                    stInOld = el.getStatement(oldDb);
                    list.set(list.indexOf(stInOld), stInNew);
                    paths.add(getRelativeFilePath(stInNew));
                    deleteStatementIfExists(stInNew);
                    break;
            }
        }

        Map<Path, StringBuilder> dumps = new HashMap<>();
        list.stream().filter(st -> paths.contains(getRelativeFilePath(st)))
                .sorted(ExportTableOrder.INSTANCE)
                .forEach(st -> dumpStatement(st, dumps));

        writeDumps(dumps);
    }

    /**
     * Exports selected objects as a new project structure.
     * Creates clean directory structure with only specified objects.
     *
     * @throws IOException if export operation fails
     */
    public void exportProject() throws IOException {
        createOutDir();

        List<IStatement> list = new ArrayList<>();
        changeList.stream().filter(el -> el.getType() != DbObjType.DATABASE)
                .forEach(el -> list.add(el.getStatement(newDb)));

        Map<Path, StringBuilder> dumps = new HashMap<>();
        list.stream()
                .sorted(ExportTableOrder.INSTANCE)
                .forEach(st -> dumpStatement(st, dumps));

        writeDumps(dumps);
    }

    private void writeDumps(Map<Path, StringBuilder> dumps) throws IOException {
        for (var dump : dumps.entrySet()) {
            dumpSQL(dump.getValue(), dump.getKey());
        }

        writeProjVersion(outDir.resolve(Consts.FILENAME_WORKING_DIR_MARKER));
    }

    protected void dumpStatement(IStatement st, Map<Path, StringBuilder> dumps) {
        Path path = outDir.resolve(getRelativeFilePath(st));
        StringBuilder sb = dumps.computeIfAbsent(path, e -> new StringBuilder());
        String dump = getDumpSql(st);

        if (dump.isEmpty()) {
            return;
        }

        if (!sb.isEmpty()) {
            sb.append(GROUP_DELIMITER);
        }

        sb.append(dump);
    }

    protected void dumpSQL(CharSequence sql, Path path) throws IOException {
        Files.createDirectories(path.getParent());
        try (PrintWriter outFile = new UnixPrintWriter(Files.newOutputStream(path,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE), sqlEncoding)) {
            outFile.println(sql);
        }
    }

    protected String getDumpSql(IStatement statement) {
        return statement.getSQL(true, settings);
    }

    /**
     * Removes file if it exists for the given statement.
     *
     * @param st the statement whose file should be deleted
     * @throws IOException if deletion fails
     */
    protected void deleteStatementIfExists(IStatement st) throws IOException {
        Path toDelete = outDir.resolve(getRelativeFilePath(st));

        if (Files.deleteIfExists(toDelete)) {
            var msg = Messages.ModelExporter_log_delete_file.formatted(toDelete, st.getStatementType(), st.getName());
            LOG.info(msg);
        }
    }

    /**
     * Gets the exported filename for a database statement.
     *
     * @param statement the database statement
     * @return sanitized filename suitable for file system
     */
    public static String getExportedFilename(IStatement statement) {
        return FileUtils.getValidFilename(statement.getBareName());
    }

    /**
     * Gets the SQL filename with .sql extension.
     *
     * @param name the base name
     * @return filename with .sql extension
     */
    public static String getExportedFilenameSql(String name) {
        return FileUtils.getValidFilename(name) + ".sql"; //$NON-NLS-1$
    }

    /**
     * Writes project version marker file.
     *
     * @param path the path to write version file
     * @throws IOException if writing fails
     */
    public static void writeProjVersion(Path path) throws IOException {
        try (UnixPrintWriter pw = new UnixPrintWriter(Files.newBufferedWriter(path, StandardCharsets.UTF_8))) {
            pw.println(Consts.VERSION_PROP_NAME + " = " //$NON-NLS-1$
                    + Consts.EXPORT_CURRENT_VERSION);
        }
    }

    /**
     * Gets the relative file path for a database statement within project structure.
     *
     * @param st the database statement
     * @return relative path for the statement's file
     */
    public static Path getRelativeFilePath(IStatement st) {
        if (st instanceof ISubElement) {
            st = st.getParent();
        }
        Path path = WorkDirs.getRelativeFolderPath(st, Paths.get("")); //$NON-NLS-1$

        String fileName = getExportedFilenameSql(getExportedFilename(st));
        if (st.getDbType() == DatabaseType.MS && st instanceof ISearchPath sp) {
            fileName = FileUtils.getValidFilename(sp.getSchemaName()) + '.' + fileName;
        }

        return path.resolve(fileName);
    }
}

/**
 * Sets fixed order for table subelements export as historically defined by DiffTree.create().
 */
final class ExportTableOrder implements Comparator<IStatement> {

    static final ExportTableOrder INSTANCE = new ExportTableOrder();

    @Override
    public int compare(IStatement o1, IStatement o2) {
        int result = Integer.compare(getTableSubElementRank(o1), getTableSubElementRank(o2));
        if (result != 0) {
            return result;
        }

        return o1.getBareName().compareTo(o2.getBareName());
    }

    private int getTableSubElementRank(IStatement el) {
        return switch (el.getStatementType()) {
            case INDEX -> 1;
            case TRIGGER -> 2;
            case RULE -> 3;
            case CONSTRAINT -> 4;
            case POLICY -> 5;
            case STATISTICS -> 6;
            default -> 0;
        };
    }

    private ExportTableOrder() {
    }
}
