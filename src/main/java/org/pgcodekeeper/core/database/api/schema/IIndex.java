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
package org.pgcodekeeper.core.database.api.schema;

import java.util.Collection;

/**
 * Interface for database index
 */
public interface IIndex extends ISubElement, ISimpleOptionContainer, ISimpleColumnContainer {

    @Override
    default DbObjType getStatementType() {
        return DbObjType.INDEX;
    }

    @Override
    default ObjectReference toObjectReference() {
        return new ObjectReference(getContainingSchema().getName(), getName(), getStatementType());
    }

    /**
     * @return true if unique index
     */
    boolean isUnique();

    /**
     * Compares the columns of this index with a collection of column references.
     *
     * @param refs the collection of column references to compare against
     * @return true if the columns match in order and count
     */
    boolean compareColumns(Collection<String> refs);
}
