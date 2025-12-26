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
package org.pgcodekeeper.core.utils;

import org.pgcodekeeper.core.database.api.schema.DatabaseType;
import org.pgcodekeeper.core.exception.PgCodeKeeperException;
import org.pgcodekeeper.core.WorkDirs;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.model.difftree.TreeElement;
import org.pgcodekeeper.core.model.exporter.ModelExporter;
import org.pgcodekeeper.core.model.exporter.OverridesModelExporter;
import org.pgcodekeeper.core.database.base.schema.AbstractDatabase;
import org.pgcodekeeper.core.settings.ISettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;

/**
 * Database project update and export management utility.
 * Handles partial and full updates of database projects with safe backup and restore functionality.
 * Supports overrides-only updates and manages temporary directories for safe atomic operations.
 */
public class ProjectUpdater {

    private static final Logger LOG = LoggerFactory.getLogger(ProjectUpdater.class);

    private final AbstractDatabase dbNew;
    private final AbstractDatabase dbOld;

    private final Collection<TreeElement> changedObjects;
    private final String encoding;
    private final Path dirExport;
    private final DatabaseType dbType;
    private final boolean overridesOnly;
    private final ISettings settings;

    /**
     * Creates a new project updater with specified configuration.
     *
     * @param dbNew          the new database schema
     * @param dbOld          the old database schema for comparison
     * @param changedObjects collection of changed tree elements
     * @param dbType         the database type
     * @param encoding       the file encoding to use
     * @param dirExport      the export directory path
     * @param overridesOnly  whether to update only overrides
     * @param settings       the application settings
     */
    public ProjectUpdater(AbstractDatabase dbNew, AbstractDatabase dbOld, Collection<TreeElement> changedObjects,
                          DatabaseType dbType, String encoding, Path dirExport, boolean overridesOnly, ISettings settings) {
        this.dbNew = dbNew;
        this.dbOld = dbOld;

        this.changedObjects = changedObjects;

        this.encoding = encoding;
        this.dirExport = dirExport;

        this.dbType = dbType;
        this.overridesOnly = overridesOnly;
        this.settings = settings;
    }

    /**
     * Performs partial update of database project.
     * Updates only changed objects with safe backup and restore on failure.
     *
     * @throws IOException if update operation fails
     */
    public void updatePartial() throws IOException {
        LOG.info(Messages.ProjectUpdater_log_start_partial_update);
        if (dbOld == null) {
            throw new IOException(Messages.ProjectUpdater_old_db_null);
        }

        boolean caughtProcessingEx = false;
        try (TempDir tmp = new TempDir(dirExport, "tmp-export")) { //$NON-NLS-1$
            Path dirTmp = tmp.get();

            try {
                updatePartial(dirTmp);
            } catch (Exception ex) {
                caughtProcessingEx = true;
                tryToRestore(dirTmp, ex);
                throw new IOException(
                        Messages.ProjectUpdater_error_update.formatted(ex.getLocalizedMessage()), ex);
            }
        } catch (IOException ex) {
            if (caughtProcessingEx) {
                // exception & err msg are already formed in the inner catch
                throw ex;
            }
            throw new IOException(
                    Messages.ProjectUpdater_error_no_tempdir.formatted(ex.getLocalizedMessage()), ex);
        }
    }


    private void tryToRestore(Path dirTmp, Exception ex) throws IOException {
        LOG.error(Messages.ProjectUpdater_log_update_err_restore_proj, ex);
        try {
            restoreProjectDir(dirTmp);
        } catch (Exception exRestore) {
            LOG.error(Messages.ProjectUpdater_log_restoring_err, exRestore);
            IOException exNew = new IOException(Messages.ProjectUpdater_error_backup_restore, exRestore);
            exNew.addSuppressed(ex);
            throw exNew;
        }
    }

    private void updatePartial(Path dirTmp) throws IOException, PgCodeKeeperException {
        LOG.info(Messages.ProjectUpdater_log_start_partial_update);
        if (overridesOnly) {
            updateFolder(dirTmp, WorkDirs.OVERRIDES);
            new OverridesModelExporter(dirExport.resolve(WorkDirs.OVERRIDES),
                    dbNew, dbOld, changedObjects, encoding, dbType, settings).exportPartial();
            return;
        }

        for (String subdirName : WorkDirs.getDirectoryNames(dbType)) {
            updateFolder(dirTmp, subdirName);
        }

        new ModelExporter(dirExport, dbNew, dbOld, dbType, changedObjects, encoding, settings).exportPartial();
    }

    private void updateFolder(Path dirTmp, String folder) throws IOException {
        final Path sourcePath = dirExport.resolve(folder);
        if (Files.exists(sourcePath)) {
            final Path targetPath = dirTmp.resolve(folder);

            Files.walkFileTree(sourcePath, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    Files.createDirectories(targetPath.resolve(sourcePath.relativize(dir)));
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.copy(file, targetPath.resolve(sourcePath.relativize(file)));
                    return FileVisitResult.CONTINUE;
                }
            });
        }
    }

    /**
     * Performs full update of database project.
     * Completely regenerates project structure with optional overrides preservation.
     *
     * @param projectOnly whether to preserve overrides directory during update
     * @throws IOException if update operation fails
     */
    public void updateFull(boolean projectOnly) throws IOException {
        LOG.info(Messages.ProjectUpdater_log_start_full_update);
        boolean caughtProcessingEx = false;
        try (TempDir tmp = new TempDir(dirExport, "tmp-export")) { //$NON-NLS-1$
            Path dirTmp = tmp.get();

            try {
                safeCleanProjectDir(dirTmp);
                new ModelExporter(dirExport, dbNew, dbType, encoding, settings).exportFull();
                if (projectOnly) {
                    restoreFolder(dirTmp, WorkDirs.OVERRIDES);
                }
            } catch (Exception ex) {
                caughtProcessingEx = true;

                tryToRestore(dirTmp, ex);
                throw new IOException(
                        Messages.ProjectUpdater_error_update.formatted(ex.getLocalizedMessage()), ex);
            }
        } catch (IOException ex) {
            if (caughtProcessingEx) {
                // exception & err msg are already formed in the inner catch
                throw ex;
            }
            throw new IOException(
                    Messages.ProjectUpdater_error_no_tempdir.formatted(ex.getLocalizedMessage()), ex);
        }
    }

    private void safeCleanProjectDir(Path dirTmp) throws IOException {
        for (String subdirName : WorkDirs.getDirectoryNames(dbType)) {
            moveFolder(dirTmp, subdirName);
        }

        moveFolder(dirTmp, WorkDirs.OVERRIDES);
    }

    private void moveFolder(Path dirTmp, String folder) throws IOException {
        Path dirOld = dirExport.resolve(folder);
        if (Files.exists(dirOld)) {
            Files.move(dirOld, dirTmp.resolve(folder), StandardCopyOption.ATOMIC_MOVE);
        }
    }

    private void restoreProjectDir(Path dirTmp) throws IOException {
        for (String subdirName : WorkDirs.getDirectoryNames(dbType)) {
            restoreFolder(dirTmp, subdirName);
        }

        restoreFolder(dirTmp, WorkDirs.OVERRIDES);
    }

    private void restoreFolder(Path dirTmp, String folder) throws IOException {
        Path subDir = dirExport.resolve(folder);
        Path subDirTemp = dirTmp.resolve(folder);

        if (Files.exists(subDirTemp)) {
            if (Files.exists(subDir)) {
                FileUtils.deleteRecursive(subDir);
            }
            Files.move(subDirTemp, subDir, StandardCopyOption.ATOMIC_MOVE);
        }
    }
}
