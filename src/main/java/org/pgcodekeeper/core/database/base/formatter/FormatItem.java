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
package org.pgcodekeeper.core.database.base.formatter;

/**
 * Represents a single formatting operation to be applied to SQL text.
 * Immutable class that describes a text modification at a specific position.
 *
 * @param start  The starting position in the original text (0-based index)
 * @param length The number of characters to replace
 * @param text   The new text to insert at the position
 */
public record FormatItem(int start, int length, String text) {

}
