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
 **
 * Copyright 2006 StartNet s.r.o.
 *
 * Distributed under MIT license
 *******************************************************************************/
package org.pgcodekeeper.core.schema;

/**
 * Interface for database objects that contain simple column references.
 * Used primarily by indexes and similar objects that reference columns with attributes.
 */
public interface ISimpleColumnContainer {
    /**
     * Adds a column reference to this container.
     *
     * @param column the simple column to add
     */
    void addColumn(SimpleColumn column);

    /**
     * Adds an included column to this container.
     *
     * @param column the column name to include
     */
    void addInclude(String column);
}
