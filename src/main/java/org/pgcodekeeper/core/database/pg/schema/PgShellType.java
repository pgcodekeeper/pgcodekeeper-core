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

import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.hasher.Hasher;

/**
 * PostgreSQL shell type implementation.
 * Shell types are placeholder types created before their actual definition,
 * used to resolve forward references in type definitions.
 */
public final class PgShellType extends PgAbstractType {

    /**
     * Creates a new PostgreSQL shell type.
     *
     * @param name shell type name
     */
    public PgShellType(String name) {
        super(name);
    }

    @Override
    protected void appendDef(StringBuilder sb) {
        // no body
    }

    @Override
    protected boolean compareUnalterable(PgAbstractType newType) {
        return true;
    }

    @Override
    public void computeHash(Hasher hasher) {
        // no impl
    }

    @Override
    public boolean compare(IStatement obj) {
        return obj instanceof PgShellType && super.compare(obj);
    }

    @Override
    protected PgAbstractType getCopy() {
        return new PgShellType(name);
    }
}
