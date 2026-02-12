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

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.script.SQLScript;

/**
 * PostgreSQL full-text search template implementation.
 * Templates define the interface for FTS dictionaries by specifying
 * the functions that dictionaries must provide for text processing.
 */
public final class PgFtsTemplate extends PgAbstractStatement implements ISearchPath {

    private String initFunction;
    private String lexizeFunction;

    /**
     * Creates a new PostgreSQL FTS template.
     *
     * @param name template name
     */
    public PgFtsTemplate(String name) {
        super(name);
    }

    @Override
    public DbObjType getStatementType() {
        return DbObjType.FTS_TEMPLATE;
    }

    @Override
    public void getCreationSQL(SQLScript script) {
        StringBuilder sbSql = new StringBuilder();
        sbSql.append("CREATE TEXT SEARCH TEMPLATE ")
                .append(getQualifiedName()).append(" (\n\t");

        if (initFunction != null) {
            sbSql.append("INIT = ").append(initFunction).append(",\n\t");
        }

        sbSql.append("LEXIZE = ").append(lexizeFunction).append(" )");
        script.addStatement(sbSql);
        appendComments(script);
    }

    @Override
    public ObjectState appendAlterSQL(IStatement newCondition, SQLScript script) {
        int startSize = script.getSize();
        var newTemplate = (PgFtsTemplate) newCondition;
        if (!compareUnalterable(newTemplate)) {
            return ObjectState.RECREATE;
        }
        appendAlterComments(newTemplate, script);

        return getObjectState(script, startSize);
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.put(initFunction);
        hasher.put(lexizeFunction);
    }

    @Override
    public boolean compare(IStatement obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof PgFtsTemplate temp && super.compare(obj)) {
            return compareUnalterable(temp);
        }

        return false;
    }

    private boolean compareUnalterable(PgFtsTemplate template) {
        return Objects.equals(initFunction, template.initFunction)
                && Objects.equals(lexizeFunction, template.lexizeFunction);
    }

    @Override
    protected PgFtsTemplate getCopy() {
        PgFtsTemplate templateDst = new PgFtsTemplate(name);
        templateDst.setInitFunction(initFunction);
        templateDst.setLexizeFunction(lexizeFunction);
        return templateDst;
    }


    public void setInitFunction(final String initFunction) {
        this.initFunction = initFunction;
        resetHash();
    }

    public void setLexizeFunction(final String lexizeFunction) {
        this.lexizeFunction = lexizeFunction;
        resetHash();
    }
}
