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
 * Interface for database statements that are represented by their full source code.
 * The source is separated into two parts by the CREATE/ALTER statement keywords,
 * allowing for flexible SQL generation while preserving the original formatting.
 */
public interface SourceStatement extends ISearchPath {
    /**
     * Gets the first part of the source statement (before CREATE/ALTER).
     *
     * @return the first part of the source
     */
    String getFirstPart();

    /**
     * Sets the first part of the source statement.
     *
     * @param firstPart the first part to set
     */
    void setFirstPart(String firstPart);

    /**
     * Gets the second part of the source statement (after the object name).
     *
     * @return the second part of the source
     */
    String getSecondPart();

    /**
     * Sets the second part of the source statement.
     *
     * @param secondPart the second part to set
     */
    void setSecondPart(String secondPart);

    /**
     * Assembles entire statement from source parts
     *
     * @param isCreate do CREATE or ALTER
     */
    default void appendSourceStatement(boolean isCreate, StringBuilder sb) {
        sb.append(getFirstPart())
                .append(isCreate ? "CREATE " : "ALTER ")
                .append(getStatementType())
                .append(' ');
        appendName(sb)
                .append(getSecondPart());
    }

    /**
     * Appends the only normalized statement part: its name and location,
     * always qualifies and quotes.
     */
    default StringBuilder appendName(StringBuilder sb) {
        return sb.append(getQualifiedName());
    }
}
