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
package org.pgcodekeeper.core.database.base.schema;

import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.database.api.schema.ISubElement;
import org.pgcodekeeper.core.hasher.Hasher;

/**
 * Abstract table rule implementation.
 */
public abstract class AbstractRule extends AbstractStatement implements ISubElement {

    @Override
    public DbObjType getStatementType() {
        return DbObjType.RULE;
    }

    /**
     * Creates a new PostgreSQL rule.
     *
     * @param name rule name
     */
    protected AbstractRule(String name) {
        super(name);
    }

    @Override
    public AbstractRule shallowCopy() {
        AbstractRule copy = getRuleCopy();
        copyBaseFields(copy);
        return copy;
    }

    protected abstract AbstractRule getRuleCopy();

    @Override
    public void computeHash(Hasher hasher) {
    }

    @Override
    public boolean compare(IStatement obj) {
        if (this == obj) {
            return true;
        }

        return obj instanceof AbstractRule rule && super.compare(rule);
    }
}
