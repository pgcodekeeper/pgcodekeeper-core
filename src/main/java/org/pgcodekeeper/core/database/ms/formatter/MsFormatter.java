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
package org.pgcodekeeper.core.database.ms.formatter;

import java.util.*;

import org.pgcodekeeper.core.database.api.formatter.IFormatConfiguration;
import org.pgcodekeeper.core.database.base.formatter.AbstractFormatter;
import org.pgcodekeeper.core.database.base.formatter.FormatItem;

/**
 * Microsoft SQL Server specific SQL formatter implementation.
 * Handles formatting of T-SQL syntax according to configured rules.
 */
public class MsFormatter extends AbstractFormatter {

    /**
     * Constructs a new Microsoft SQL Server formatter instance.
     *
     * @param source The source SQL text to format
     * @param offset Starting offset in the source text
     * @param length Length of text to format
     * @param config Formatting configuration options
     */
    public MsFormatter(String source, int offset, int length, IFormatConfiguration config) {
        super(source, offset, length, config);
    }

    @Override
    public List<FormatItem> getFormatItems() {
        return Collections.emptyList();
    }
}