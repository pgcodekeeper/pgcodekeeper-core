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
 **
 * Copyright 2006 StartNet s.r.o.
 *
 * Distributed under MIT license
 *******************************************************************************/
package org.pgcodekeeper.core.database.pg.schema;

import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.base.schema.AbstractFunction;

import java.util.Objects;

/**
 * PostgreSQL function implementation.
 * Represents a regular PostgreSQL function (as opposed to procedures or aggregates)
 * with a return type and function body.
 */
public final class PgFunction extends PgAbstractFunction {

    private String returns;

    @Override
    public DbObjType getStatementType() {
        return DbObjType.FUNCTION;
    }

    /**
     * Creates a new PostgreSQL function.
     *
     * @param name function name
     */
    public PgFunction(String name) {
        super(name);
    }

    @Override
    public boolean needDrop(AbstractFunction newFunction) {
        if (newFunction == null ||
                !Objects.equals(getReturns(), newFunction.getReturns())) {
            return true;
        }
        return super.needDrop(newFunction);
    }

    @Override
    protected PgAbstractFunction getFunctionCopy() {
        return new PgFunction(getBareName());
    }

    @Override
    public String getReturns() {
        return returns;
    }

    @Override
    public void setReturns(String returns) {
        this.returns = returns;
        resetHash();
    }

    @Override
    protected boolean compareUnalterable(AbstractFunction function) {
        if (function instanceof PgFunction pgFunc && super.compareUnalterable(function)) {
            return Objects.equals(returns, pgFunc.getReturns());
        }
        return false;
    }

    @Override
    public void computeHash(Hasher hasher) {
        super.computeHash(hasher);
        hasher.put(returns);
    }
}
