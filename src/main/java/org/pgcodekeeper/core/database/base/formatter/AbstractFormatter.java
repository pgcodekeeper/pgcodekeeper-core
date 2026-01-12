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
package org.pgcodekeeper.core.database.base.formatter;

import org.pgcodekeeper.core.database.api.formatter.IFormatConfiguration;

import java.util.Collections;
import java.util.List;

/**
 * Abstract base class for SQL formatter implementations.
 * Provides common functionality and structure for database-specific formatters.
 */
public abstract class AbstractFormatter {

    protected final String source;
    protected final int start;
    protected final int stop;
    protected final IFormatConfiguration config;

    protected AbstractFormatter(String source, int offset, int length, IFormatConfiguration config) {
        this.source = source;
        this.start = offset;
        this.stop = offset + length;
        this.config = config;
    }

    protected abstract List<FormatItem> getFormatItems();

    /**
     * Formats the source text according to configuration and database type.
     *
     * @return Formatted SQL string
     */
    public String formatText() {
        List<FormatItem> list = getFormatItems();
        if (list.isEmpty()) {
            return source;
        }

        Collections.reverse(list);
        var sb = new StringBuilder(source);
        for (var item : list) {
            var itemStart = item.start();
            sb.replace(itemStart, itemStart + item.length(), item.text());
        }

        return sb.toString();
    }
}