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
package org.pgcodekeeper.core.database.base.schema.meta;

import java.util.*;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.api.schema.ICast.CastContext;
import org.pgcodekeeper.core.database.api.schema.meta.IMetaContainer;

/**
 * Container for database metadata objects organized by type and schema.
 * Provides efficient lookup and storage of functions, operators, relations, constraints,
 * casts, and composite types from database schemas.
 */
public class MetaContainer implements IMetaContainer {

    private final List<ICast> casts = new ArrayList<>();

    /**
     * Functions grouped by schema name
     */
    private final Map<String, Map<String, IFunction>> functions = new LinkedHashMap<>();

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
    private final Map<String, Map<String, List<IConstraintPk>>> primaryKeys = new LinkedHashMap<>();

    /**
     * Composite types grouped by schema name
     */
    private final Map<String, Map<String, ICompositeType>> types = new LinkedHashMap<>();

    /**
     * Adds a statement to the appropriate collection based on its type.
     *
     * @param st the statement to add
     */
    public void addStatement(IStatement st) {
        if (st instanceof IFunction f) {
            functions.computeIfAbsent(f.getSchemaName(), e -> new LinkedHashMap<>()).put(f.getName(), f);
            return;
        }
        if (st instanceof ICast cast && cast.getContext() == CastContext.IMPLICIT) {
            casts.add(cast);
            return;
        }
        if (st instanceof IOperator oper) {
            operators.computeIfAbsent(oper.getSchemaName(), e -> new LinkedHashMap<>()).put(oper.getName(), oper);
            return;
        }
        if (st instanceof IRelation rel) {
            relations.computeIfAbsent(rel.getSchemaName(), e -> new LinkedHashMap<>()).put(rel.getName(), rel);
            return;
        }
        if (st instanceof IConstraintPk con) {
            primaryKeys
                    .computeIfAbsent(con.getSchemaName(), e -> new LinkedHashMap<>())
                    .computeIfAbsent(con.getTableName(), e -> new ArrayList<>())
                    .add(con);
            return;
        }
        if (st instanceof ICompositeType t) {
            types.computeIfAbsent(t.getSchemaName(), e -> new LinkedHashMap<>()).put(t.getName(), t);
        }
    }

    @Override
    public boolean containsCastImplicit(String source, String target) {
        for (ICast cast : casts) {
            if (source.equals(cast.getSource())
                    && target.equals(cast.getTarget())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public IRelation findRelation(String schemaName, String relationName) {
        return relations.getOrDefault(schemaName, Collections.emptyMap()).get(relationName);
    }

    @Override
    public Map<String, Map<String, IRelation>> getRelations() {
        return Collections.unmodifiableMap(relations);
    }

    @Override
    public IFunction findFunction(String schemaName, String functionName) {
        return functions.getOrDefault(schemaName, Collections.emptyMap()).get(functionName);
    }

    @Override
    public Collection<IFunction> availableFunctions(String schemaName) {
        return Collections.unmodifiableCollection(functions
                .getOrDefault(schemaName, Collections.emptyMap()).values());
    }

    @Override
    public IOperator findOperator(String schemaName, String operatorName) {
        return operators.getOrDefault(schemaName, Collections.emptyMap()).get(operatorName);
    }

    @Override
    public ICompositeType findType(String schemaName, String typeName) {
        return types.getOrDefault(schemaName, Collections.emptyMap()).get(typeName);
    }

    @Override
    public Collection<IOperator> availableOperators(String schemaName) {
        return Collections.unmodifiableCollection(operators
                .getOrDefault(schemaName, Collections.emptyMap()).values());
    }

    @Override
    public Collection<IConstraintPk> getPrimaryKeys(String schemaName, String tableName) {
        return primaryKeys
                .getOrDefault(schemaName, Collections.emptyMap())
                .getOrDefault(tableName, Collections.emptyList());
    }
}
