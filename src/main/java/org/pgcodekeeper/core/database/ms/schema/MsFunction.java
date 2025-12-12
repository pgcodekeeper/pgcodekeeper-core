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
package org.pgcodekeeper.core.database.ms.schema;

import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.base.schema.AbstractFunction;

import java.util.Objects;

/**
 * Represents a Microsoft SQL user-defined function.
 * Supports scalar, table-valued, and other function types.
 */
public final class MsFunction extends MsAbstractFunction {

    private MsFunctionTypes funcType = MsFunctionTypes.SCALAR;

    @Override
    public DbObjType getStatementType() {
        return DbObjType.FUNCTION;
    }

    /**
     * Creates a new Microsoft SQL function.
     *
     * @param name the function name
     */
    public MsFunction(String name) {
        super(name);
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
    protected boolean compareUnalterable(AbstractFunction func) {
        return func instanceof MsAbstractFunction && super.compareUnalterable(func)
                && Objects.equals(funcType, ((MsFunction) func).funcType);
    }

    @Override
    public boolean needDrop(AbstractFunction newFunction) {
        if (newFunction instanceof MsFunction msFunction) {
            return funcType != msFunction.funcType;
        }

        return true;
    }

    @Override
    public void computeHash(Hasher hasher) {
        super.computeHash(hasher);
        hasher.put(funcType);
    }

    @Override
    protected MsAbstractFunction getFunctionCopy() {
        MsFunction func = new MsFunction(name);
        func.setFuncType(funcType);
        return func;
    }

    public void setFuncType(MsFunctionTypes funcType) {
        this.funcType = funcType;
        resetHash();
    }
}
