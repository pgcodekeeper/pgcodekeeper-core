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
package org.pgcodekeeper.core.database.api.formatter;

/**
 * Interface for formatting configuration
 */
public interface IFormatConfiguration {

    /**
     * @return indent type
     * @see IndentType
     */
    IndentType getIndentType();

    /**
     * @return indent size
     */
    int getIndentSize();

    /**
     * Creates an indentation string based on current configuration.
     *
     * @param length desired length of indentation
     * @return indentation string
     */
    String createIndent(int length);

    /**
     * @return true if space should be added after operators
     */
    boolean isAddWhitespaceAfterOp();

    /**
     * @return true if space should be added before operators
     */
    boolean isAddWhitespaceBeforeOp();

    /**
     * @return true if trailing spaces should be removed
     */
    boolean isRemoveTrailingWhitespace();
}
