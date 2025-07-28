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
package org.pgcodekeeper.core.schema;

import java.util.Collection;
import java.util.stream.Stream;

/**
 * Interface representing a database schema containing tables, views, functions, and other objects.
 * Extends statement container functionality with schema-specific operations.
 */
public interface ISchema extends IStatementContainer {
    /**
     * Gets all relations (tables, views, sequences) in this schema.
     *
     * @return a stream of relations
     */
    Stream<IRelation> getRelations();

    /**
     * Gets a relation by name.
     *
     * @param name the relation name
     * @return the relation with the given name, or null if not found
     */
    IRelation getRelation(String name);

    /**
     * Gets all functions in this schema.
     *
     * @return a collection of functions
     */
    Collection<IFunction> getFunctions();

    /**
     * Gets a function by its signature.
     *
     * @param signature the function signature
     * @return the function with the given signature, or null if not found
     */
    IFunction getFunction(String signature);
}
