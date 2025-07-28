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
package org.pgcodekeeper.core.schema.pg;

import org.pgcodekeeper.core.schema.AbstractColumn;
import org.pgcodekeeper.core.schema.AbstractTable;
import org.pgcodekeeper.core.script.SQLScript;

/**
 * PostgreSQL simple foreign table implementation.
 * Represents a basic foreign table that accesses data from external sources
 * through foreign data wrappers without complex inheritance or special features.
 *
 * @author galiev_mr
 * @since 4.1.1
 */
public final class SimpleForeignPgTable extends AbstractForeignTable {

    /**
     * Creates a new PostgreSQL simple foreign table.
     *
     * @param name       table name
     * @param serverName foreign server name this table connects to
     */
    public SimpleForeignPgTable(String name, String serverName) {
        super(name, serverName);
    }

    @Override
    protected AbstractTable getTableCopy() {
        return new SimpleForeignPgTable(name, serverName);
    }

    @Override
    protected void appendColumns(StringBuilder sbSQL, SQLScript script) {
        sbSQL.append(" (\n");

        int start = sbSQL.length();
        for (AbstractColumn column : columns) {
            writeColumn((PgColumn) column, sbSQL, script);
        }

        if (start != sbSQL.length()) {
            sbSQL.setLength(sbSQL.length() - 2);
            sbSQL.append('\n');
        }

        sbSQL.append(')');
    }
}