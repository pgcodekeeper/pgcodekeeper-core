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

/**
 * Interface for function argument.
 */
public interface IArgument {

    String getDataType();

    String getDefaultExpression();

    void setDefaultExpression(final String defaultExpression);

    boolean isReadOnly();

    void setReadOnly(final boolean isReadOnly);

    ArgMode getMode();

    String getName();

    /**
     * Appends the argument declaration to a StringBuilder.
     *
     * @param sbString            the StringBuilder to append to
     * @param includeDefaultValue whether to include the default value
     * @param includeArgName      whether to include the argument name
     */
    void appendDeclaration(StringBuilder sbString, boolean includeDefaultValue, boolean includeArgName);
}
