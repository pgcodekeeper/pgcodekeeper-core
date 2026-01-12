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
package org.pgcodekeeper.core.database.ms.schema;

import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.base.schema.AbstractFunction;

/**
 * Represents a Microsoft SQL stored procedure.
 * Supports execution of SQL statements and business logic within the database.
 */
public final class MsProcedure extends MsAbstractFunction {

    /**
     * Creates a new Microsoft SQL stored procedure.
     *
     * @param name the procedure name
     */
    public MsProcedure(String name) {
        super(name);
    }

    @Override
    public DbObjType getStatementType() {
        return DbObjType.PROCEDURE;
    }

    @Override
    protected void appendFunctionFullSQL(StringBuilder sbSQL, boolean isCreate) {
        sbSQL.append("SET QUOTED_IDENTIFIER ").append(isQuotedIdentified() ? "ON" : "OFF");
        sbSQL.append(GO).append('\n');
        sbSQL.append("SET ANSI_NULLS ").append(isAnsiNulls() ? "ON" : "OFF");
        sbSQL.append(GO).append('\n');

        appendSourceStatement(isCreate, sbSQL);
    }

    @Override
    public boolean needDrop(AbstractFunction newFunction) {
        return newFunction instanceof MsClrProcedure;
    }

    @Override
    protected MsAbstractFunction getFunctionCopy() {
        return new MsProcedure(name);
    }
}
