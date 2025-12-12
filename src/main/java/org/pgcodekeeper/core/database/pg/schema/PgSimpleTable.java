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
package org.pgcodekeeper.core.database.pg.schema;

import org.pgcodekeeper.core.database.base.schema.AbstractColumn;
import org.pgcodekeeper.core.database.base.schema.AbstractTable;
import org.pgcodekeeper.core.script.SQLScript;

/**
 * PostgreSQL simple table implementation.
 * Represents a standard PostgreSQL table with columns, constraints,
 * and typical table features without special inheritance or typing.
 *
 * @author galiev_mr
 * @since 4.1.1
 */
public final class PgSimpleTable extends PgAbstractRegularTable {

    /**
     * Creates a new PostgreSQL simple table.
     *
     * @param name table name
     */
    public PgSimpleTable(String name) {
        super(name);
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

    @Override
    protected AbstractTable getTableCopy() {
        return new PgSimpleTable(name);
    }

    @Override
    protected void compareTableTypes(PgAbstractTable newTable, SQLScript script) {
        if (newTable instanceof PgAbstractRegularTable regTable) {
            regTable.convertTable(script);
        }
    }

    @Override
    protected void convertTable(SQLScript script) {
        // no implements
    }
}