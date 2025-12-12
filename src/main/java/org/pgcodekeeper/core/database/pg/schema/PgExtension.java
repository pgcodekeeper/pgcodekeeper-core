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

import org.pgcodekeeper.core.PgDiffUtils;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.base.schema.AbstractDatabase;
import org.pgcodekeeper.core.database.api.schema.ObjectState;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.script.SQLScript;

import java.util.Objects;

/**
 * PostgreSQL extension implementation.
 * Extensions are add-on modules that provide additional functionality
 * to PostgreSQL databases, such as data types, functions, and operators.
 *
 * @author Alexander Levsha
 */
public final class PgExtension extends AbstractStatement {

    private String schema;
    private boolean relocatable;

    @Override
    public DbObjType getStatementType() {
        return DbObjType.EXTENSION;
    }

    /**
     * Creates a new PostgreSQL extension.
     *
     * @param name extension name
     */
    public PgExtension(String name) {
        super(name);
    }

    /**
     * Gets the schema where this extension is installed.
     *
     * @return schema name or null if using default
     */
    public String getSchema() {
        return schema;
    }

    public void setSchema(final String schema) {
        this.schema = schema;
        resetHash();
    }

    public void setRelocatable(boolean relocatable) {
        this.relocatable = relocatable;
    }

    @Override
    public AbstractDatabase getDatabase() {
        return (AbstractDatabase) parent;
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        final StringBuilder sbSQL = new StringBuilder();
        sbSQL.append("CREATE EXTENSION ");
        appendIfNotExists(sbSQL, script.getSettings());
        sbSQL.append(getQualifiedName());

        if (schema != null && !schema.isEmpty()) {
            sbSQL.append(" SCHEMA ");
            sbSQL.append(schema);
        }

        script.addStatement(sbSQL);
        appendComments(script);
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        int startSize = script.getSize();
        PgExtension newExt = (PgExtension) newCondition;
        boolean isNeedDepcies = false;
        if (!Objects.equals(newExt.schema, getSchema())) {
            if (!relocatable) {
                return ObjectState.RECREATE;
            }
            StringBuilder sql = new StringBuilder();
            sql.append("ALTER EXTENSION ")
                    .append(PgDiffUtils.getQuotedName(name))
                    .append(" SET SCHEMA ")
                    .append(newExt.getSchema());
            script.addStatement(sql);
            isNeedDepcies = true;
        }
        // TODO ALTER EXTENSION UPDATE TO ?

        appendAlterComments(newExt, script);
        return getObjectState(isNeedDepcies, script, startSize);
    }

    @Override
    public boolean compare(IStatement obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof PgExtension ext && super.compare(obj)) {
            return Objects.equals(schema, ext.schema);
        }

        return false;
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.put(schema);
    }

    @Override
    public PgExtension shallowCopy() {
        PgExtension extDst = new PgExtension(name);
        copyBaseFields(extDst);
        extDst.setSchema(schema);
        return extDst;
    }
}
