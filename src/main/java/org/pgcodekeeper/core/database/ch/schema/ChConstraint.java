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
package org.pgcodekeeper.core.database.ch.schema;

import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.database.api.schema.ObjectState;
import org.pgcodekeeper.core.database.base.schema.AbstractConstraint;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;

import java.util.Objects;

/**
 * Represents a ClickHouse table constraint (CHECK or ASSUME).
 */
public final class ChConstraint extends AbstractConstraint implements IChStatement {

    private final boolean isAssume;
    private String expr;

    /**
     * Creates a new ClickHouse constraint.
     *
     * @param name     the name of the constraint
     * @param isAssume true if this is an ASSUME constraint, false for CHECK constraint
     */
    public ChConstraint(String name, boolean isAssume) {
        super(name);
        this.isAssume = isAssume;
    }

    public void setExpr(String expr) {
        this.expr = expr;
        resetHash();
    }

    @Override
    public String getDefinition() {
        return (isAssume ? "ASSUME " : "CHECK ") + expr;
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        final StringBuilder sb = new StringBuilder();
        appendAlterTable(sb);
        sb.append(" ADD CONSTRAINT ").append(getQuotedName(name)).append(' ').append(getDefinition());
        script.addStatement(sb);
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        var newConstr = (ChConstraint) newCondition;
        if (!compareUnalterable(newConstr)) {
            return ObjectState.RECREATE;
        }
        return ObjectState.NOTHING;
    }

    @Override
    public void getDropSQL(SQLScript script, boolean optionExists) {
        final StringBuilder sb = new StringBuilder();
        appendAlterTable(sb);
        sb.append("\n\tDROP CONSTRAINT ");
        if (optionExists) {
            sb.append(IF_EXISTS);
        }
        sb.append(getQuotedName(name));
        script.addStatement(sb);
    }

    private boolean compareUnalterable(ChConstraint newConstr) {
        return Objects.equals(isAssume, newConstr.isAssume)
                && Objects.equals(expr, newConstr.expr);
    }

    @Override
    public void computeHash(Hasher hasher) {
        super.computeHash(hasher);
        hasher.put(isAssume);
        hasher.put(expr);
    }

    @Override
    public boolean compare(IStatement obj) {
        if (this == obj) {
            return true;
        }
        return obj instanceof ChConstraint constr && super.compare(constr)
                && compareUnalterable(constr);
    }

    @Override
    protected AbstractConstraint getConstraintCopy() {
        var constr = new ChConstraint(name, isAssume);
        constr.setExpr(expr);
        return constr;
    }
}