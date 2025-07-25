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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.pgcodekeeper.core.DatabaseType;
import org.pgcodekeeper.core.PgCodekeeperException;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.model.difftree.TreeElement;
import org.pgcodekeeper.core.model.difftree.TreeElement.DiffSide;
import org.pgcodekeeper.core.schema.AbstractColumn;
import org.pgcodekeeper.core.schema.AbstractDatabase;
import org.pgcodekeeper.core.schema.AbstractTable;
import org.pgcodekeeper.core.schema.PgPrivilege;
import org.pgcodekeeper.core.schema.PgStatement;
import org.pgcodekeeper.core.script.SQLScript;
import org.pgcodekeeper.core.settings.ISettings;

public final class OverridesModelExporter extends ModelExporter {

    public OverridesModelExporter(Path outDir, AbstractDatabase newDb, AbstractDatabase oldDb,
            Collection<TreeElement> changedObjects, String sqlEncoding, DatabaseType dbType, ISettings settings) {
        super(outDir, newDb, oldDb, dbType, changedObjects, sqlEncoding, settings);
    }

    @Override
    public void exportFull() throws IOException {
        throw new IllegalStateException();
    }

    @Override
    public void exportPartial() throws IOException, PgCodekeeperException {
        if (oldDb == null) {
            throw new PgCodekeeperException("Old database should not be null for partial export.");
        }
        if (Files.notExists(outDir)) {
            Files.createDirectories(outDir);
        } else if (!Files.isDirectory(outDir)) {
            throw new NotDirectoryException(outDir.toString());
        }

        List<PgStatement> list = oldDb.getDescendants().collect(Collectors.toList());
        Set<Path> paths = new HashSet<>();

        for (TreeElement el : changeList) {
            if (el.getSide() == DiffSide.BOTH) {
                switch (el.getType()) {
                case CONSTRAINT, DATABASE, INDEX, TRIGGER, RULE, POLICY, EXTENSION, EVENT_TRIGGER, CAST, COLUMN,
                STATISTICS:
                    break;
                default:
                    PgStatement stInNew = el.getPgStatement(newDb);
                    PgStatement stInOld = el.getPgStatement(oldDb);
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
    protected String getDumpSql(PgStatement st) {
        SQLScript script = new SQLScript(settings);
        Set<PgPrivilege> privs = st.getPrivileges();
        st.appendOwnerSQL(script);
        PgPrivilege.appendPrivileges(privs, script);
        if (privs.isEmpty() && st.getDbType() == DatabaseType.PG) {
            PgPrivilege.appendDefaultPostgresPrivileges(st, script);
        }

        if (DbObjType.TABLE == st.getStatementType()) {
            for (AbstractColumn col : ((AbstractTable)st).getColumns()) {
                PgPrivilege.appendPrivileges(col.getPrivileges(), script);
            }
        }
        return script.getFullScript();
    }
}
