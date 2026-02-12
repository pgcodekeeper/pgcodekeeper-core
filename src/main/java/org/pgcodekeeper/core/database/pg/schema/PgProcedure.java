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
import org.pgcodekeeper.core.hasher.Hasher;

/**
 * PostgreSQL stored procedure implementation.
 * Procedures are similar to functions but can manage transactions
 * and don't return values directly (though they can have OUT parameters).
 */
public final class PgProcedure extends PgAbstractFunction {

    private String returns;

    /**
     * Creates a new PostgreSQL procedure.
     *
     * @param name procedure name
     */
    public PgProcedure(String name) {
        super(name);
    }

    @Override
    public DbObjType getStatementType() {
        return DbObjType.PROCEDURE;
    }

    @Override
    public String getReturns() {
        return returns;
    }

    @Override
    public void setReturns(String returns) {
        this.returns = returns;
    }

    @Override
    public void computeHash(Hasher hasher) {
        super.computeHash(hasher);
        hasher.put(returns);
    }

    @Override
    protected boolean compareUnalterable(PgAbstractFunction function) {
        if (function instanceof PgProcedure proc && super.compareUnalterable(function)) {
            return Objects.equals(returns, proc.getReturns());
        }
        return false;
    }

    @Override
    protected PgAbstractFunction getFunctionCopy() {
        return new PgProcedure(name);
    }
}
