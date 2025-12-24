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

import org.pgcodekeeper.core.database.pg.PgDiffUtils;
import org.pgcodekeeper.core.hasher.Hasher;
import org.pgcodekeeper.core.hasher.IHashable;
import org.pgcodekeeper.core.hasher.JavaHasher;

import java.util.Objects;

/**
 * Represents a table inheritance relationship in PostgreSQL.
 * Contains the schema and table name of a parent table that is inherited by another table.
 *
 * @param key   the inherits key
 * @param value the inherits value
 */
public record Inherits(String key, String value) implements IHashable {

    /**
     * Gets the qualified name of the inherited table.
     *
     * @return the qualified table name in the format schema.table
     */
    public String getQualifiedName() {
        return (key == null ? "" : (PgDiffUtils.getQuotedName(key) + '.')) + PgDiffUtils.getQuotedName(value);
    }

    @Override
    public int hashCode() {
        JavaHasher hasher = new JavaHasher();
        computeHash(hasher);
        return hasher.getResult();
    }

    @Override
    public void computeHash(Hasher hasher) {
        hasher.put(key);
        hasher.put(value);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof Inherits other
                && Objects.equals(key, other.key)
                && Objects.equals(value, other.value);
    }
}