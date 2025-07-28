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

import org.pgcodekeeper.core.schema.*;
import org.pgcodekeeper.core.schema.ICast.CastContext;
import org.pgcodekeeper.core.schema.ch.ChFunction;

import java.util.*;

/**
 * Container for database metadata objects organized by type and schema.
 * Provides efficient lookup and storage of functions, operators, relations, constraints,
 * casts, and composite types from database schemas.
 */
public final class MetaContainer {

    private final List<ICast> casts = new ArrayList<>();

    /**
     * Functions grouped by schema name
     */
    private final Map<String, Map<String, IFunction>> functions = new LinkedHashMap<>();

    /**
     * ClickHouse functions
     */
    private final Map<String, IFunction> chFunctions = new LinkedHashMap<>();

    /**
     * Operators grouped by schema name
     */
    private final Map<String, Map<String, IOperator>> operators = new LinkedHashMap<>();

    /**
     * Relations grouped by schema name
     */
    private final Map<String, Map<String, IRelation>> relations = new LinkedHashMap<>();

    /**
     * Constraints grouped by schema name and table name
     */
    private final Map<String, Map<String, List<IConstraint>>> constraints = new LinkedHashMap<>();

    /**
     * PostgreSQL composite types grouped by schema name
     */
    private final Map<String, Map<String, MetaCompositeType>> pgCompositeTypes = new LinkedHashMap<>();

    /**
     * Adds a statement to the appropriate collection based on its type.
     *
     * @param st the statement to add
     */
    public void addStatement(IStatement st) {
        switch (st.getStatementType()) {
            case CAST:
                casts.add((ICast) st);
                break;
            case FUNCTION, PROCEDURE, AGGREGATE:
                if (st instanceof ChFunction f) {
                    chFunctions.put(f.getName(), f);
                    break;
                }
                IFunction f = (IFunction) st;
                functions.computeIfAbsent(f.getSchemaName(), e -> new LinkedHashMap<>()).put(f.getName(), f);
                break;
            case OPERATOR:
                IOperator op = (IOperator) st;
                operators.computeIfAbsent(op.getSchemaName(), e -> new LinkedHashMap<>()).put(op.getName(), op);
                break;
            case TABLE, DICTIONARY, VIEW, SEQUENCE:
                IRelation rel = (IRelation) st;
                relations.computeIfAbsent(rel.getSchemaName(), e -> new LinkedHashMap<>()).put(rel.getName(), rel);
                break;
            case CONSTRAINT:
                IConstraint con = (IConstraint) st;
                constraints
                        .computeIfAbsent(con.getSchemaName(), e -> new LinkedHashMap<>())
                        .computeIfAbsent(con.getTableName(), e -> new ArrayList<>())
                        .add(con);
                break;
            case TYPE:
                if (st instanceof MetaCompositeType t) {
                    pgCompositeTypes.computeIfAbsent(t.getSchemaName(), e -> new LinkedHashMap<>()).put(t.getName(), t);
                }
                break;
            default:
                break;
        }
    }

    /**
     * Checks if there is an implicit cast from source to target type.
     *
     * @param source the source data type
     * @param target the target data type
     * @return true if an implicit cast exists, false otherwise
     */
    public boolean containsCastImplicit(String source, String target) {
        for (ICast cast : casts) {
            if (CastContext.IMPLICIT == cast.getContext()
                    && source.equals(cast.getSource())
                    && target.equals(cast.getTarget())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Finds a relation by schema and name.
     *
     * @param schemaName   the schema name
     * @param relationName the relation name
     * @return the relation, or null if not found
     */
    public IRelation findRelation(String schemaName, String relationName) {
        return relations.getOrDefault(schemaName, Collections.emptyMap()).get(relationName);
    }

    /**
     * Returns all relations grouped by schema name.
     *
     * @return unmodifiable map of relations
     */
    public Map<String, Map<String, IRelation>> getRelations() {
        return Collections.unmodifiableMap(relations);
    }

    /**
     * Finds a function by schema and name.
     *
     * @param schemaName   the schema name
     * @param functionName the function name
     * @return the function, or null if not found
     */
    public IFunction findFunction(String schemaName, String functionName) {
        return functions.getOrDefault(schemaName, Collections.emptyMap()).get(functionName);
    }

    /**
     * Checks if a ClickHouse function exists.
     *
     * @param functionName the function name
     * @return true if the function exists, false otherwise
     */
    public boolean findChFunction(String functionName) {
        return chFunctions.containsKey(functionName);
    }

    /**
     * Returns all available ClickHouse functions.
     *
     * @return unmodifiable collection of ClickHouse functions
     */
    public Collection<IFunction> availableChFunctions() {
        return Collections.unmodifiableCollection(chFunctions.values());
    }

    /**
     * Returns available functions in the specified schema.
     *
     * @param schemaName the schema name
     * @return unmodifiable collection of functions
     */
    public Collection<IFunction> availableFunctions(String schemaName) {
        return Collections.unmodifiableCollection(functions
                .getOrDefault(schemaName, Collections.emptyMap()).values());
    }

    /**
     * Finds an operator by schema and name.
     *
     * @param schemaName   the schema name
     * @param operatorName the operator name
     * @return the operator, or null if not found
     */
    public IOperator findOperator(String schemaName, String operatorName) {
        return operators.getOrDefault(schemaName, Collections.emptyMap()).get(operatorName);
    }

    /**
     * Finds a PostgreSQL composite type by schema and name.
     *
     * @param schemaName the schema name
     * @param typeName   the type name
     * @return the composite type, or null if not found
     */
    public MetaCompositeType findType(String schemaName, String typeName) {
        return pgCompositeTypes.getOrDefault(schemaName, Collections.emptyMap()).get(typeName);
    }

    /**
     * Returns available operators in the specified schema.
     *
     * @param schemaName the schema name
     * @return unmodifiable collection of operators
     */
    public Collection<IOperator> availableOperators(String schemaName) {
        return Collections.unmodifiableCollection(operators
                .getOrDefault(schemaName, Collections.emptyMap()).values());
    }

    /**
     * Returns constraints for the specified table.
     *
     * @param schemaName the schema name
     * @param tableName  the table name
     * @return collection of constraints for the table
     */
    public Collection<IConstraint> getConstraints(String schemaName, String tableName) {
        return constraints
                .getOrDefault(schemaName, Collections.emptyMap())
                .getOrDefault(tableName, Collections.emptyList());
    }
}
