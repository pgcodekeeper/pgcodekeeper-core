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
package org.pgcodekeeper.core.database.api.schema.meta;

import java.util.Collection;
import java.util.Map;

import org.pgcodekeeper.core.database.api.schema.ICompositeType;
import org.pgcodekeeper.core.database.api.schema.IConstraintPk;
import org.pgcodekeeper.core.database.api.schema.IFunction;
import org.pgcodekeeper.core.database.api.schema.IOperator;
import org.pgcodekeeper.core.database.api.schema.IRelation;

/**
 * Interface for a container of database metadata objects
 */
public interface IMetaContainer {

    /**
     * Finds a relation by schema and name.
     *
     * @param schemaName   the schema name
     * @param relationName the relation name
     * @return the relation, or null if not found
     */
    IRelation findRelation(String schemaName, String relationName);

    /**
     * Returns available functions in the specified schema.
     *
     * @param schemaName the schema name
     * @return unmodifiable collection of functions
     */
    Collection<IFunction> availableFunctions(String schemaName);

    /**
     * Returns available operators in the specified schema.
     *
     * @param schemaName the schema name
     * @return unmodifiable collection of operators
     */
    Collection<IOperator> availableOperators(String schemaName);

    /**
     * Finds composite type by schema and name.
     *
     * @param schemaName the schema name
     * @param typeName   the type name
     * @return the composite type, or null if not found
     */
    ICompositeType findType(String schemaName, String typeName);

    /**
     * Finds a function by schema and name.
     *
     * @param schemaName   the schema name
     * @param functionName the function name
     * @return the function, or null if not found
     */
    IFunction findFunction(String schemaName, String functionName);

    /**
     * Finds an operator by schema and name.
     *
     * @param schemaName   the schema name
     * @param operatorName the operator name
     * @return the operator, or null if not found
     */
    IOperator findOperator(String schemaName, String operatorName);

    /**
     * Returns primary keys for the specified table.
     *
     * @param schemaName the schema name
     * @param tableName  the table name
     * @return collection of primary keys for the table
     */
    Collection<IConstraintPk> getPrimaryKeys(String schemaName, String tableName);

    /**
     * Checks if there is an implicit cast from source to target type.
     *
     * @param source the source data type
     * @param target the target data type
     * @return true if an implicit cast exists, false otherwise
     */
    boolean containsCastImplicit(String source, String target);

    /**
     * Returns all relations grouped by schema name.
     *
     * @return unmodifiable map of relations
     */
    Map<String, Map<String, IRelation>> getRelations();
}