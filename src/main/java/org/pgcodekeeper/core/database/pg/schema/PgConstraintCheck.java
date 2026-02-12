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

import java.util.Objects;

import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.hasher.Hasher;

/**
 * PostgreSQL CHECK constraint implementation.
 * CHECK constraints enforce domain integrity by limiting the values
 * that can be placed in a column based on a Boolean expression.
 */
public final class PgConstraintCheck extends PgConstraint {

    private boolean isInherit = true;
    private String expression;

    /**
     * Creates a new PostgreSQL CHECK constraint.
     *
     * @param name constraint name
     */
    public PgConstraintCheck(String name) {
        super(name);
    }

    public void setInherit(boolean isInherit) {
        this.isInherit = isInherit;
        resetHash();
    }

    public void setExpression(String expression) {
        this.expression = expression;
        resetHash();
    }

    @Override
    public String getDefinition() {
        var sbSQL = new StringBuilder();
        sbSQL.append("CHECK (").append(expression).append(')');
        if (!isInherit) {
            sbSQL.append(" NO INHERIT");
        }
        return sbSQL.toString();
    }

    @Override
    public void computeHash(Hasher hasher) {
        super.computeHash(hasher);
        hasher.put(isInherit);
        hasher.put(expression);
    }

    @Override
    protected boolean compareUnalterable(PgConstraint newConstr) {
        if (newConstr instanceof PgConstraintCheck con) {
            return super.compareUnalterable(con)
                    && isInherit == con.isInherit
                    && Objects.equals(expression, con.expression);
        }
        return false;
    }

    @Override
    protected PgConstraint getConstraintCopy() {
        var con = new PgConstraintCheck(name);
        con.setInherit(isInherit);
        con.setExpression(expression);
        return con;
    }

    @Override
    public String getErrorCode() {
        return Consts.DUPLICATE_OBJECT;
    }
}