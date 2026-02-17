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
import org.pgcodekeeper.core.database.api.schema.IFunction;
import org.pgcodekeeper.core.database.api.schema.IStatement;

/**
 * Represents a Microsoft SQL stored procedure.
 * Supports execution of SQL statements and business logic within the database.
 */
public class MsProcedure extends MsAbstractFunction {

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
        appendSourceStatement(sbSQL, isQuotedIdentified(), isAnsiNulls(), isCreate);
    }

    @Override
    public boolean needDrop(IFunction newFunction) {
        return newFunction instanceof MsClrProcedure;
    }

    @Override
    protected MsAbstractFunction getFunctionCopy() {
        return new MsProcedure(name);
    }

    @Override
    public boolean compare(IStatement obj) {
        return this == obj || super.compare(obj);
    }
}
