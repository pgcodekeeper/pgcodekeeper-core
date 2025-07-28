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

/**
 * Interface for database operators.
 * Defines functionality for custom operators including argument types and return type.
 */
public interface IOperator extends ISearchPath {
    /**
     * Gets the right argument type of this operator.
     *
     * @return the right argument type
     */
    String getRightArg();

    /**
     * Gets the left argument type of this operator.
     *
     * @return the left argument type
     */
    String getLeftArg();

    /**
     * Gets the return type of this operator.
     *
     * @return the return type
     */
    String getReturns();

    /**
     * Sets the return type of this operator.
     *
     * @param returns the return type to set
     */
    void setReturns(String returns);
}
