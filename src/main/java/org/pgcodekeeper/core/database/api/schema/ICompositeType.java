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

import java.util.List;

import org.pgcodekeeper.core.utils.Pair;

/**
 * Represents composite type metadata object.
 * Stores information about composite type attributes including their names and types.
 *
 * @see IType Base interface for all type metadata objects
 */
public interface ICompositeType extends IType {

    public default String getSchemaName() {
        return getParent().getName();
    }

    /**
     * Returns the type of the specified attribute.
     *
     * @param attrName the attribute name
     * @return the attribute type, or null if not found
     */
    String getAttrType(String attrName);

    /**
     * Returns list of all attributes.
     *
     * @return list of attributes
     */
    List<Pair<String, String>> getAttrs();
}
