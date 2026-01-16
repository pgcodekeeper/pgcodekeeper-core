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
package org.pgcodekeeper.core.model.exporter;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.exception.PgCodeKeeperException;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.database.pg.schema.PgPrivilege;
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
 * Model exporter for database object overrides.
 * Exports only ownership and privileges information for changed objects,
 * used for partial exports that focus on access control modifications.
 */
public final class OverridesModelExporter extends ModelExporter {

    /**
     * Creates a new overrides model exporter.
     *
     * @param outDir         output directory for export
     * @param newDb          new database schema
     * @param oldDb          old database schema
     * @param changedObjects collection of changed tree elements
     * @param sqlEncoding    SQL file encoding
     * @param dbType         database type
     * @param settings       export settings
     */
    public OverridesModelExporter(Path outDir, IDatabase newDb, IDatabase oldDb, Collection<TreeElement> changedObjects,
                                  String sqlEncoding, DatabaseType dbType, ISettings settings) {
        super(outDir, newDb, oldDb, dbType, changedObjects, sqlEncoding, settings);
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
    protected String getDumpSql(IStatement st) {
        SQLScript script = new SQLScript(settings);
        Set<IPrivilege> privs = st.getPrivileges();
        st.appendOwnerSQL(script);
        IPrivilege.appendPrivileges(privs, script);
        if (privs.isEmpty() && st.getDbType() == DatabaseType.PG) {
            PgPrivilege.appendDefaultPostgresPrivileges(st, script);
        }

        if (DbObjType.TABLE == st.getStatementType()) {
            for (AbstractColumn col : ((AbstractTable) st).getColumns()) {
                IPrivilege.appendPrivileges(col.getPrivileges(), script);
            }
        }
        return script.getFullScript();
    }
}
