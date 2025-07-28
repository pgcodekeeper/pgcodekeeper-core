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
package org.pgcodekeeper.core.schema.meta;

import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.schema.GenericColumn;
import org.pgcodekeeper.core.schema.IRelation;
import org.pgcodekeeper.core.schema.ISchema;
import org.pgcodekeeper.core.schema.PgObjLocation;
import org.pgcodekeeper.core.utils.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Represents a database relation metadata object (table, view, sequence, etc.).
 * Stores information about relation columns including their names and types.
 */
public final class MetaRelation extends MetaStatement implements IRelation {

    private static final long serialVersionUID = -6057721257178245883L;

    /**
     * Contains columns names and types
     */
    private List<Pair<String, String>> columns;

    /**
     * Creates a new relation metadata object with location information.
     *
     * @param object the object location information
     */
    public MetaRelation(PgObjLocation object) {
        super(object);
    }

    /**
     * Creates a new relation metadata object.
     *
     * @param schemaName   the schema name
     * @param relationName the relation name
     * @param type         the database object type
     */
    public MetaRelation(String schemaName, String relationName, DbObjType type) {
        super(new GenericColumn(schemaName, relationName, type));
    }

    @Override
    public Stream<Pair<String, String>> getRelationColumns() {
        return columns == null ? null : columns.stream();
    }

    /**
     * Sets the columns for this relation.
     *
     * @param columns the list of column name-type pairs
     */
    public void addColumns(List<? extends Pair<String, String>> columns) {
        this.columns = new ArrayList<>();
        this.columns.addAll(columns);
    }

    /**
     * Returns the containing schema of this relation.
     * This operation is not supported for metadata relations.
     *
     * @return never returns normally
     * @throws IllegalStateException always thrown as this operation is unsupported
     */
    @Override
    public ISchema getContainingSchema() {
        throw new IllegalStateException("Unsupported operation");
    }

    /**
     * Returns the schema name of this relation.
     *
     * @return the schema name
     */
    @Override
    public String getSchemaName() {
        return getObject().getSchema();
    }
}