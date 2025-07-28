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

import java.util.List;
import java.util.Map;

/**
 * Interface for database functions, procedures, and aggregates.
 * Provides access to function arguments, return types, and return columns.
 */
public interface IFunction extends ISearchPath {
    /**
     * Gets the return type of this function.
     *
     * @return the return type, or null if not applicable
     */
    String getReturns();

    /**
     * Gets the return columns for table-valued functions.
     *
     * @return a map of column names to their types
     */
    Map<String, String> getReturnsColumns();

    /**
     * Gets the list of function arguments.
     *
     * @return the list of arguments
     */
    List<Argument> getArguments();

    /**
     * Sets the return type of this function.
     *
     * @param returns the return type to set
     */
    void setReturns(String returns);
}