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
 **
 * Copyright 2006 StartNet s.r.o.
 *
 * Distributed under MIT license
 *******************************************************************************/
package org.pgcodekeeper.core.database.pg.schema;

import java.util.Objects;

import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.api.schema.IFunction;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.hasher.Hasher;

/**
 * PostgreSQL function implementation.
 * Represents a regular PostgreSQL function (as opposed to procedures or aggregates)
 * with a return type and function body.
 */
public class PgFunction extends PgAbstractFunction {

    private String returns;

    /**
     * Creates a new PostgreSQL function.
     *
     * @param name function name
     */
    public PgFunction(String name) {
        super(name);
    }

    @Override
    public DbObjType getStatementType() {
        return DbObjType.FUNCTION;
    }

    @Override
    public String getReturns() {
        return returns;
    }

    @Override
    public boolean needDrop(IFunction newFunction) {
        if (newFunction == null ||
                !Objects.equals(getReturns(), newFunction.getReturns())) {
            return true;
        }
        return super.needDrop(newFunction);
    }

    @Override
    public void setReturns(String returns) {
        this.returns = returns;
        resetHash();
    }

    @Override
    public void computeHash(Hasher hasher) {
        super.computeHash(hasher);
        hasher.put(returns);
    }

    @Override
    public boolean compare(IStatement obj) {
        return this == obj || super.compare(obj);
    }

    @Override
    protected boolean compareUnalterable(PgAbstractFunction function) {
        return function instanceof PgFunction pgFunc && super.compareUnalterable(function)
                && Objects.equals(returns, pgFunc.getReturns());
    }

    @Override
    protected PgAbstractFunction getFunctionCopy() {
        return new PgFunction(getBareName());
    }
}
