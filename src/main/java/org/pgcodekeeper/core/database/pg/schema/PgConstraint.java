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

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.Collection;
import java.util.Collections;

/**
 * Base PostgreSQL constraint implementation.
 * Provides common functionality for all PostgreSQL constraint types
 * including deferrable and initially deferred properties.
 */
public abstract class PgConstraint extends PgAbstractStatement implements IConstraint, ISubElement {

    protected boolean deferrable;
    protected boolean initially;
    protected boolean notEnforced;
    protected boolean isNotValid;

    /**
     * Creates a new PostgreSQL constraint.
     *
     * @param name constraint name
     */
    protected PgConstraint(String name) {
        super(name);
    }

    public void setDeferrable(boolean deferrable) {
        this.deferrable = deferrable;
        resetHash();
    }

    public void setInitially(boolean initially) {
        this.initially = initially;
        resetHash();
    }

    public void setNotEnforced(boolean notEnforced) {
        this.notEnforced = notEnforced;
        resetHash();
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        final StringBuilder sbSQL = new StringBuilder();
        appendAlterTable(sbSQL);
        sbSQL.append("\n\tADD");
        if (!name.isEmpty()) {
            sbSQL.append(" CONSTRAINT ").append(getQuotedName());
        }
        sbSQL.append(' ');
        sbSQL.append(getDefinition());
        if (deferrable) {
            sbSQL.append(" DEFERRABLE");
        }
        if (initially) {
            sbSQL.append(" INITIALLY DEFERRED");
        }
        if (notEnforced) {
            sbSQL.append(" NOT ENFORCED");
        }

        appendOptions(script, sbSQL, true);
    }

    protected void appendAlterTable(StringBuilder sb) {
        sb.append("ALTER ").append(parent.getStatementType().name()).append(' ');
        sb.append(getParent().getQualifiedName());
    }

    public void appendOptions(SQLScript script, StringBuilder sbSQL, boolean needNotValid) {
        boolean generateNotValid = isGenerateNotValid(script.getSettings());

        if (needNotValid && (isNotValid || generateNotValid)) {
            sbSQL.append(" NOT VALID");
        }
        sbSQL.append(';');

        if (generateNotValid && !isNotValid) {
            sbSQL.append("\n\n");
            appendAlterTable(sbSQL);
            sbSQL.append(" VALIDATE CONSTRAINT ")
                    .append(getQuotedName())
                    .append(';');
        }

        appendExtraOptions(sbSQL);

        if (script.getSettings().isGenerateExistDoBlock()) {
            StringBuilder sb = new StringBuilder();
            appendSqlWrappedInDo(sb, sbSQL, getErrorCode());
            script.addStatement(sb);
        } else {
            sbSQL.setLength(sbSQL.length() - 1);
            if (!sbSQL.isEmpty()) {
                script.addStatement(sbSQL);
            }
        }
        appendComments(script);
    }

    protected boolean isGenerateNotValid(ISettings settings) {
        if (!settings.isGenerateConstraintNotValid()) {
            return false;
        }
        if (parent instanceof PgPartitionTable) {
            return false;
        }
        return parent instanceof PgAbstractRegularTable regTable && regTable.getPartitionBy() == null;
    }

    protected void appendExtraOptions(StringBuilder sbSQL) {
        // subclasses will override if needed
    }

    @Override
    public void getDropSQL(SQLScript script, boolean optionExists) {
        final StringBuilder sbSQL = new StringBuilder();
        appendAlterTable(sbSQL);
        sbSQL.append("\n\tDROP CONSTRAINT ");
        if (optionExists) {
            sbSQL.append(IF_EXISTS);
        }
        sbSQL.append(getQuotedName());
        script.addStatement(sbSQL);
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        int startSize = script.getSize();
        PgConstraint newConstr = (PgConstraint) newCondition;

        if (!compareUnalterable(newConstr)) {
            return ObjectState.RECREATE;
        }

        if (isNotValid && !newConstr.isNotValid) {
            StringBuilder sbSQL = new StringBuilder();
            appendAlterTable(sbSQL);
            sbSQL.append("\n\tVALIDATE CONSTRAINT ").append(getQuotedName());
            script.addStatement(sbSQL);
        }

        compareExtraOptions(newConstr, script);
        appendAlterComments(newConstr, script);
        return getObjectState(script, startSize);
    }

    protected void compareExtraOptions(PgConstraint newConstr, SQLScript script) {
        // subclasses will override if needed
    }

    @Override
    protected void appendCommentSql(SQLScript script) {
        StringBuilder sb = new StringBuilder();
        sb.append("COMMENT ON CONSTRAINT ");
        sb.append(getQuotedName()).append(" ON ");
        if (parent.getStatementType() == DbObjType.DOMAIN) {
            sb.append("DOMAIN ");
        }
        sb.append(parent.getQualifiedName()).append(" IS ").append(checkComments() ? comment : "NULL");
        script.addCommentStatement(sb.toString());
    }

    public void setNotValid(boolean isNotValid) {
        this.isNotValid = isNotValid;
        resetHash();
    }

    public boolean isNotValid() {
        return isNotValid;
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.put(isNotValid);
        hasher.put(deferrable);
        hasher.put(initially);
        hasher.put(notEnforced);
    }

    @Override
    public boolean compare(IStatement obj) {
        return obj instanceof PgConstraint con && super.compare(obj)
                && compareUnalterable(con)
                && isNotValid == con.isNotValid;
    }

    protected boolean compareUnalterable(PgConstraint con) {
        return deferrable == con.deferrable
                && initially == con.initially
                && notEnforced == con.notEnforced;
    }

    @Override
    protected PgConstraint getCopy() {
        PgConstraint con = getConstraintCopy();
        con.setNotValid(isNotValid);
        con.setDeferrable(deferrable);
        con.setInitially(initially);
        con.setNotEnforced(notEnforced);
        return con;
    }

    @Override
    public String getTableName() {
        return parent.getName();
    }

    @Override
    public boolean containsColumn(String name) {
        return getColumns().contains(name);
    }

    @Override
    public Collection<String> getColumns() {
        return Collections.emptySet();
    }

    protected abstract String getErrorCode();

    protected abstract PgConstraint getConstraintCopy();
}
