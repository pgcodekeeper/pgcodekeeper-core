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
package org.pgcodekeeper.core.formatter.ms;

import org.pgcodekeeper.core.formatter.AbstractFormatter;
import org.pgcodekeeper.core.formatter.FormatConfiguration;
import org.pgcodekeeper.core.formatter.FormatItem;

import java.util.Collections;
import java.util.List;

/**
 * Microsoft SQL Server specific SQL formatter implementation.
 * Handles formatting of T-SQL syntax according to configured rules.
 */
public class MsFormatter extends AbstractFormatter {

    /**
     * Constructs a new Microsoft SQL Server formatter instance.
     *
     * @param source The source SQL text to be formatted
     * @param start  The starting offset in the source text for formatting
     * @param stop   The ending offset in the source text for formatting
     * @param config The formatting configuration containing style rules
     */
    public MsFormatter(String source, int start, int stop, FormatConfiguration config) {
        super(source, start, stop, config);
    }

    @Override
    public List<FormatItem> getFormatItems() {
        return Collections.emptyList();
    }
}