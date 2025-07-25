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
package org.pgcodekeeper.core.formatter;

/**
 * Enumeration of indentation directions used during SQL formatting.
 * Determines how indentation should be applied to different parts of SQL statements.
 */
public enum IndentDirection {
    /**
     * First token in new block
     */
    BLOCK_START,
    /**
     * Last token in block
     */
    BLOCK_STOP,
    /**
     * New line in block
     */
    BLOCK_LINE,
    /**
     * Forced new line
     */
    NEW_LINE
}