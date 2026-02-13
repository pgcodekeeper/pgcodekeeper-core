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
package org.pgcodekeeper.core.database.base.project;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.exception.PgCodeKeeperException;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.model.difftree.TreeElement;
import org.pgcodekeeper.core.model.difftree.TreeElement.DiffSide;
import org.pgcodekeeper.core.script.SQLScript;
import org.pgcodekeeper.core.settings.ISettings;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * Abstract base class for database object override exporters.
 * Exports only ownership and privileges information for changed objects,
 * used for partial exports that focus on access control modifications.
 * <p>
 * Subclasses must implement database-specific methods for directory structure and file paths.
 */
public abstract class AbstractOverridesModelExporter extends AbstractModelExporter {

    /**
     * Creates a new overrides model exporter.
     *
     * @param outDir         output directory for export
     * @param newDb          new database schema
     * @param oldDb          old database schema
     * @param changedObjects collection of changed tree elements
     * @param sqlEncoding    SQL file encoding
     * @param settings       export settings
     */
    protected AbstractOverridesModelExporter(Path outDir, IDatabase newDb, IDatabase oldDb,
                                             Collection<TreeElement> changedObjects,
                                             String sqlEncoding, ISettings settings) {
        super(outDir, newDb, oldDb, changedObjects, sqlEncoding, settings);
    }

    @Override
    public void exportFull() {
        throw new IllegalStateException();
    }

    /**
     * Exports ownership and privileges for partial object changes.
     * Only processes objects with BOTH side differences, ignoring structural changes.
     *
     * @throws IOException           if export operation fails
     * @throws PgCodeKeeperException if old database is null
     */
    @Override
    public void exportPartial() throws IOException, PgCodeKeeperException {
        if (oldDb == null) {
            throw new PgCodeKeeperException(Messages.ModelExporter_log_old_database_not_null);
        }
        if (Files.notExists(outDir)) {
            Files.createDirectories(outDir);
        } else if (!Files.isDirectory(outDir)) {
            throw new NotDirectoryException(outDir.toString());
        }

        List<IStatement> list = oldDb.getDescendants().collect(Collectors.toList());
        Set<Path> paths = new HashSet<>();

        for (TreeElement el : changeList) {
            if (el.getSide() == DiffSide.BOTH) {
                switch (el.getType()) {
                    case CONSTRAINT, DATABASE, INDEX, TRIGGER, RULE, POLICY, EXTENSION, EVENT_TRIGGER, CAST, COLUMN,
                         STATISTICS:
                        break;
                    default:
                        var stInNew = el.getStatement(newDb);
                        var stInOld = el.getStatement(oldDb);
                        list.set(list.indexOf(stInOld), stInNew);
                        paths.add(getRelativeFilePath(stInNew));
                        deleteStatementIfExists(stInNew);
                }
            }
        }

        Map<Path, StringBuilder> dumps = new HashMap<>();
        list.stream()
                .filter(st -> paths.contains(getRelativeFilePath(st)))
                .forEach(st -> dumpStatement(st, dumps));

        for (Entry<Path, StringBuilder> dump : dumps.entrySet()) {
            dumpSQL(dump.getValue(), dump.getKey());
        }
    }

    @Override
    public String getDumpSql(IStatement st) {
        SQLScript script = new SQLScript(settings, st.getSeparator());
        Set<IPrivilege> privs = st.getPrivileges();
        st.appendOwnerSQL(script);
        IPrivilege.appendPrivileges(privs, script);
        appendDefaultPrivileges(st, privs, script);

        if (st instanceof ITable table) {
            for (IColumn col : table.getColumns()) {
                IPrivilege.appendPrivileges(col.getPrivileges(), script);
            }
        }
        return script.getFullScript();
    }

    protected void appendDefaultPrivileges(IStatement st, Set<IPrivilege> privileges, SQLScript script) {
        // subclasses will override if needed
    }
}
