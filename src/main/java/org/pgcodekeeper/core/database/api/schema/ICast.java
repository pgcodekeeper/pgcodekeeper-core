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

import java.util.function.UnaryOperator;

/**
 * Interface for database type cast operations.
 * Defines functionality for casting between different data types in various contexts.
 */
public interface ICast extends IStatement {

    /**
     * Template for cast names in the format "source AS target".
     */
    String CAST_NAME = "%s AS %s";

    /**
     * Indicates what contexts the cast can be invoked in.
     */
    enum CastContext {
        EXPLICIT,
        ASSIGNMENT,
        IMPLICIT
    }

    /**
     * Gets the source type of the cast.
     *
     * @return the source type name
     */
    String getSource();

    /**
     * Gets the target type of the cast.
     *
     * @return the target type name
     */
    String getTarget();

    /**
     * Gets the context in which this cast can be invoked.
     *
     * @return the cast context
     */
    CastContext getContext();

    /**
     * Creates a simple name for a cast from source to target type.
     *
     * @param source the source type
     * @param target the target type
     * @return the formatted cast name
     */
    static String getSimpleName(String source, String target) {
        return CAST_NAME.formatted(source, target);
    }

    @Override
    default GenericColumn toGenericColumn(DbObjType type) {
        return new GenericColumn(getSimpleName(getSource(), getTarget()), type);
    }

    @Override
    default UnaryOperator<String> getQuoter() {
        return (name) -> name;
    }
}
