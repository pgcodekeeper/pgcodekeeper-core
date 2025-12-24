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

import org.pgcodekeeper.core.database.pg.PgDiffUtils;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.database.api.schema.ISubElement;
import org.pgcodekeeper.core.database.api.schema.ObjectState;
import org.pgcodekeeper.core.database.base.schema.AbstractPolicy;
import org.pgcodekeeper.core.database.base.schema.AbstractSchema;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;

import java.util.Objects;
import java.util.Set;

/**
 * PostgreSQL row security policy implementation.
 * Policies control which rows are visible or modifiable for specific users or roles,
 * providing fine-grained access control at the row level.
 */
public final class PgPolicy extends AbstractPolicy implements ISubElement, IPgStatement {

    private String check;

    /**
     * Creates a new PostgreSQL policy.
     *
     * @param name policy name
     */
    public PgPolicy(String name) {
        super(name);
    }

    public void setCheck(String check) {
        this.check = check;
        resetHash();
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        final StringBuilder sbSQL = new StringBuilder();
        sbSQL.append("CREATE POLICY ");
        appendFullName(sbSQL);

        if (!isPermissive) {
            sbSQL.append("\n  AS RESTRICTIVE");
        }

        if (event != null) {
            sbSQL.append("\n  FOR ").append(event);
        }

        if (!roles.isEmpty()) {
            sbSQL.append("\n  TO ").append(String.join(", ", roles));
        }

        if (using != null && !using.isEmpty()) {
            sbSQL.append("\n  USING ").append(using);
        }

        if (check != null && !check.isEmpty()) {
            sbSQL.append("\n  WITH CHECK ").append(check);
        }
        script.addStatement(sbSQL);
        appendComments(script);
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        int startSize = script.getSize();
        PgPolicy newPolice = (PgPolicy) newCondition;

        if (!compareUnalterable(newPolice)) {
            return ObjectState.RECREATE;
        }

        Set<String> newRoles = newPolice.roles;
        String newUsing = newPolice.using;
        String newCheck = newPolice.check;

        if (!Objects.equals(roles, newRoles) || !Objects.equals(using, newUsing)
                || !Objects.equals(check, newCheck)) {
            StringBuilder sbSql = new StringBuilder();
            sbSql.append("ALTER POLICY ");
            appendFullName(sbSql);

            if (!Objects.equals(roles, newRoles)) {
                sbSql.append("\n  TO ");
                if (newRoles.isEmpty()) {
                    sbSql.append("PUBLIC");
                } else {
                    sbSql.append(String.join(", ", newRoles));
                }
            }

            if (!Objects.equals(using, newUsing)) {
                sbSql.append("\n  USING ").append(newUsing);
            }

            if (!Objects.equals(check, newCheck)) {
                sbSql.append("\n  WITH CHECK ").append(newCheck);
            }
            script.addStatement(sbSql);
        }
        appendAlterComments(newPolice, script);

        return getObjectState(script, startSize);
    }

    @Override
    protected StringBuilder appendFullName(StringBuilder sb) {
        sb.append(PgDiffUtils.getQuotedName(name));
        sb.append(" ON ").append(parent.getQualifiedName());
        return sb;
    }

    @Override
    protected AbstractPolicy getPolicyCopy() {
        PgPolicy copy = new PgPolicy(name);
        copy.setCheck(check);
        return copy;
    }

    @Override
    public boolean compare(IStatement obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof PgPolicy police && super.compare(obj)) {
            return Objects.equals(check, police.check);
        }

        return false;
    }

    private boolean compareUnalterable(PgPolicy police) {
        // we can alter but cannot remove
        if (using != null && police.using == null) {
            return false;
        }

        if (check != null && police.check == null) {
            return false;
        }

        return event == police.event && isPermissive == police.isPermissive;
    }

    @Override
    public void computeHash(Hasher hasher) {
        super.computeHash(hasher);
        hasher.put(check);
    }

    @Override
    public AbstractSchema getContainingSchema() {
        return (AbstractSchema) parent.getParent();
    }
}
