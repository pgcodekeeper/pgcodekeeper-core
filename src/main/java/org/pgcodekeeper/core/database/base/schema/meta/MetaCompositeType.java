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
package org.pgcodekeeper.core.database.base.schema.meta;

import org.pgcodekeeper.core.database.api.schema.ObjectLocation;
import org.pgcodekeeper.core.utils.Pair;

import java.io.Serial;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents a PostgreSQL composite type metadata object.
 * Stores information about composite type attributes including their names and types.
 */
public final class MetaCompositeType extends MetaStatement {

    @Serial
    private static final long serialVersionUID = 2037382156455950170L;

    private final List<Pair<String, String>> attrs = new ArrayList<>();

    /**
     * Creates a new composite type metadata object.
     *
     * @param object the object location information
     */
    public MetaCompositeType(ObjectLocation object) {
        super(object);
    }

    /**
     * Returns the schema name of this composite type.
     *
     * @return the schema name
     */
    public String getSchemaName() {
        return getObject().getSchema();
    }

    /**
     * Adds an attribute to this composite type.
     *
     * @param name the attribute name
     * @param type the attribute type
     */
    public void addAttr(String name, String type) {
        attrs.add(new Pair<>(name, type));
    }

    /**
     * Returns the type of the specified attribute.
     *
     * @param attrName the attribute name
     * @return the attribute type, or null if not found
     */
    public String getAttrType(String attrName) {
        return attrs.stream()
                .filter(pair -> pair.getFirst().equals(attrName))
                .findAny()
                .map(Pair::getSecond)
                .orElse(null);
    }

    public List<Pair<String, String>> getAttrs() {
        return Collections.unmodifiableList(attrs);
    }
}
