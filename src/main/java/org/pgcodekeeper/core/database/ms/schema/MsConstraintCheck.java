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

import java.util.Objects;

import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.hasher.Hasher;

/**
 * Represents a Microsoft SQL CHECK constraint that validates column values against a boolean expression.
 */
public class MsConstraintCheck extends MsConstraint {

    private boolean isNotForRepl;
    private String expression;

    /**
     * Creates a new Microsoft SQL CHECK constraint.
     *
     * @param name the constraint name
     */
    public MsConstraintCheck(String name) {
        super(name);
    }

    @Override
    public String getDefinition() {
        var sbSQL = new StringBuilder();
        sbSQL.append("CHECK  ");
        if (isNotForRepl) {
            sbSQL.append("NOT FOR REPLICATION ");
        }
        sbSQL.append('(').append(expression).append(')');
        return sbSQL.toString();
    }

    public void setNotForRepl(boolean isNotForRepl) {
        this.isNotForRepl = isNotForRepl;
        resetHash();
    }

    public void setExpression(String expression) {
        this.expression = expression;
        resetHash();
    }

    @Override
    public void computeHash(Hasher hasher) {
        super.computeHash(hasher);
        hasher.put(isNotForRepl);
        hasher.put(expression);
    }

    @Override
    public boolean compare(IStatement obj) {
        if (this == obj) {
            return true;
        }
        return obj instanceof MsConstraintCheck check
                && super.compare(obj)
                && compareUnalterable(check);
    }

    @Override
    protected boolean compareUnalterable(MsConstraint newConstr) {
        return newConstr instanceof MsConstraintCheck con
                && isNotForRepl == con.isNotForRepl
                && Objects.equals(expression, con.expression);
    }

    @Override
    protected MsConstraint getConstraintCopy() {
        var con = new MsConstraintCheck(name);
        con.setNotForRepl(isNotForRepl);
        con.setExpression(expression);
        return con;
    }
}