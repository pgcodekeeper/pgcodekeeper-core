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
package org.pgcodekeeper.core.database.pg.schema;

import java.util.*;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;
import org.pgcodekeeper.core.settings.ISettings;

/**
 * Base implementation of foreign table for PostgreSQL database.
 * Foreign tables are used to access data that exists outside the database,
 * typically in other databases or external data sources through foreign data wrappers.
 *
 * @author galiev_mr
 * @since 4.1.1
 */
public abstract class PgAbstractForeignTable extends PgAbstractTable implements IForeignTable, PgForeignOptionContainer {

    protected final String serverName;

    protected PgAbstractForeignTable(String name, String serverName) {
        super(name);
        this.serverName = serverName;
    }

    @Override
    protected String getAlterTable(boolean only) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER FOREIGN TABLE ");
        if (only) {
            sb.append("ONLY ");
        }
        sb.append(getQualifiedName());
        return sb.toString();
    }

    @Override
    protected boolean isNeedRecreate(PgAbstractTable newTable) {
        return super.isNeedRecreate(newTable)
                || !this.getClass().equals(newTable.getClass())
                || !Objects.equals(serverName, ((PgAbstractForeignTable) newTable).serverName);
    }

    @Override
    public void appendOptions(StringBuilder sqlOption) {
        sqlOption.append("\nSERVER ").append(quote(serverName));
        if (!options.isEmpty()) {
            sqlOption.append('\n');
        }
        PgForeignOptionContainer.super.appendOptions(sqlOption);
    }

    @Override
    public String getTypeName() {
        return "FOREIGN TABLE";
    }

    @Override
    public String getAlterHeader() {
        return getAlterTable(false);
    }

    @Override
    protected void appendName(StringBuilder sbSQL, ISettings settings) {
        sbSQL.append("CREATE FOREIGN TABLE ");
        appendIfNotExists(sbSQL, settings);
        sbSQL.append(getQualifiedName());
    }

    @Override
    protected void appendAlterOptions(SQLScript script) {
        if (hasOids) {
            script.addStatement(getAlterTable(true) + " SET WITH OIDS");
        }
    }

    @Override
    protected PgSequence writeSequences(PgColumn column, StringBuilder sbOption) {
        PgSequence sequence = super.writeSequences(column, sbOption);
        if (!sequence.isLogged()) {
            sbOption.append("\nALTER SEQUENCE ").append(sequence.getQualifiedName()).append(" SET UNLOGGED;");
        }
        return sequence;
    }

    @Override
    protected void compareTableTypes(PgAbstractTable newTable, SQLScript script) {
        // untransformable
    }

    @Override
    protected boolean compareTable(PgAbstractTable obj) {
        return obj instanceof PgAbstractForeignTable table
                && super.compareTable(table)
                && Objects.equals(serverName, table.serverName);
    }

    @Override
    public void computeHash(Hasher hasher) {
        super.computeHash(hasher);
        hasher.put(serverName);
    }

    @Override
    public void appendMoveDataSql(IStatement newCondition, SQLScript script, String tblTmpBareName,
                                  List<String> identityCols) {
        // no impl
    }
}
