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

import org.pgcodekeeper.core.database.api.schema.DatabaseType;
import org.pgcodekeeper.core.formatter.ch.ChFormatter;
import org.pgcodekeeper.core.formatter.ms.MsFormatter;
import org.pgcodekeeper.core.formatter.pg.PgFormatter;

import java.util.Collections;
import java.util.List;

/**
 * Main formatter class that handles SQL file formatting for different database types.
 * Acts as a facade that delegates to specific database formatter implementations.
 */
public class FileFormatter {

    private final String source;
    private final int start;
    private final int stop;
    private final DatabaseType dbType;

    private final FormatConfiguration config;

    /**
     * Constructs a new FileFormatter instance.
     *
     * @param source The source SQL text to format
     * @param offset Starting offset in the source text
     * @param length Length of text to format
     * @param config Formatting configuration options
     * @param dbType Target database type for dialect-specific formatting
     */
    public FileFormatter(String source, int offset, int length, FormatConfiguration config, DatabaseType dbType) {
        this.source = source;
        this.start = offset;
        this.stop = offset + length;
        this.config = config;
        this.dbType = dbType;
    }

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
            var itemStart = item.getStart();
            sb.replace(itemStart, itemStart + item.getLength(), item.getText());
        }

        return sb.toString();
    }

    /**
     * Gets the list of formatting operations that would be applied.
     * This allows inspection of formatting changes without actually modifying the text.
     *
     * @return List of FormatItem objects representing formatting operations
     */
    public List<FormatItem> getFormatItems() {
        AbstractFormatter formatter = switch (dbType) {
            case CH -> new ChFormatter(source, start, stop, config);
            case PG -> new PgFormatter(source, start, stop, config);
            case MS -> new MsFormatter(source, start, stop, config);
        };

        return formatter.getFormatItems();
    }

    public static String formatSql(String sql, DatabaseType dbType) {
        return new FileFormatter(sql, 0, sql.length(), FormatConfiguration.getDefaultConfig(), dbType).formatText();
    }
}