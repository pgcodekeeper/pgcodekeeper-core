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
package org.pgcodekeeper.core.database.base.schema;

import org.pgcodekeeper.core.database.api.schema.IColumn;
import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.api.schema.IStatement;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * Utility class providing common functionality for database statement operations.
 * Contains helper methods for column ordering, SQL generation, and option handling
 * across different database types.
 */
public final class StatementUtils {

    /**
     * Checks if the order of the table columns has changed.
     *
     * <b>Example:</b>
     * <p>
     * original columns : c1, c2, c3<br>
     * new columns      : c2, c3, c1
     * <p>
     * Column c1 was moved to last index and method will return true
     *
     * <b>Example:</b>
     * <p>
     * original columns : c1, c2, c3<br>
     * new columns      : c2, c3, c4
     * <p>
     * Column c1 was deleted and column c4 was added. Method will return false.
     *
     * <b>Example:</b>
     * <p>
     * original columns : c1, c2, c3<br>
     * new columns      : c1, c4, c2, c3
     * <p>
     * Column c4 was added between old columns: c1 and c2. Method will return true.
     *
     * <b>Example:</b>
     * <p>
     * original columns : c2, c3, inherit(some table)<br>
     * new columns      : c1, c2, c3
     * <p>
     * Some table is no longer inherited. If table did not have a column c1,
     * we must return true, but we cannot track this right now. Method will return false.
     *
     * @param newColumns new columns
     * @param oldColumns old columns
     * @return true if order was changed or order is ignored
     * @since 5.1.7
     */
    public static boolean isColumnsOrderChanged(List<? extends IColumn> newColumns,
                                                List<? extends IColumn> oldColumns) {
        // last founded column
        int i = -1;
        for (IColumn col : newColumns) {
            // old column index
            int index = 0;
            // search old column index by new column name
            for (; index < oldColumns.size(); index++) {
                if (col.getName().equals(oldColumns.get(index).getName())) {
                    break;
                }
            }

            if (index == oldColumns.size()) {
                // New column was not found in original table.
                // After this column can be only new columns.
                i = Integer.MAX_VALUE;
            } else if (index < i) {
                // New column was found in original table
                // but one of previous columns was not found
                // or was located on more later index
                return true;
            } else {
                // New column was found in original table.
                // Safe index of column in original table.
                i = index;
            }
        }

        return false;
    }

    /**
     * Appends column names to a StringBuilder with proper quoting for the database type.
     *
     * @param sbSQL the StringBuilder to append to
     * @param cols the collection of column names
     * @param quoter quoting operator
     */
    public static void appendCols(StringBuilder sbSQL, Collection<String> cols, UnaryOperator<String> quoter) {
        sbSQL.append('(');
        for (var col : cols) {
            sbSQL.append(quoter.apply(col));
            sbSQL.append(", ");
        }
        sbSQL.setLength(sbSQL.length() - 2);
        sbSQL.append(')');
    }

    /**
     * Appends options to a StringBuilder enclosed in parentheses.
     *
     * @param sbSQL the StringBuilder to append to
     * @param options the map of options to append
     * @param delimiter option/value delimiter
     */
    public static void appendOptionsWithParen(StringBuilder sbSQL, Map<String, String> options, String delimiter) {
        sbSQL.append(" (");
        appendOptions(sbSQL, options, delimiter);
        sbSQL.append(')');
    }

    /**
     * Appends a collection of strings to a StringBuilder with a specified delimiter.
     *
     * @param sbSQL the StringBuilder to append to
     * @param collection the collection of strings to append
     * @param delimiter the delimiter to use between elements
     * @param needParens whether to enclose the result in parentheses
     */
    public static void appendCollection(StringBuilder sbSQL, Collection<String> collection,
                                        String delimiter, boolean needParens) {
        if (collection.isEmpty()) {
            return;
        }

        if (needParens) {
            sbSQL.append(" (");
        }
        for (var element : collection) {
            sbSQL.append(element).append(delimiter);
        }
        sbSQL.setLength(sbSQL.length() - delimiter.length());
        if (needParens) {
            sbSQL.append(')');
        }
    }

    /**
     * Appends parameters/options at StringBuilder. This StringBuilder used in
     * schema package Constraint's classes in the method getDefinition()
     *
     * @param sbSQL      the StringBuilder from method getDefinition()
     * @param options    the Map&lt;String, String&gt; where key is parameter/option and
     *                   value is value of this parameter/option
     * @param delimiter  option/value delimiter
     */
    public static void appendOptions(StringBuilder sbSQL, Map<String, String> options, String delimiter) {
        for (var option : options.entrySet()) {
            sbSQL.append(option.getKey());
            var value = option.getValue();
            if (value != null && !value.isEmpty()) {
                sbSQL.append(delimiter).append(value);
            }
            sbSQL.append(", ");
        }
        sbSQL.setLength(sbSQL.length() - 2);
    }

    /**
     * Gets the full bare name of a statement by concatenating parent names.
     * Returns a dot-delimited path from the top-level container down to the statement,
     * excluding the database level.
     *
     * @param st the statement to get the full bare name for
     * @return the full bare name path (e.g., "schema.table.column")
     */
    public static String getFullBareName(IStatement st) {
        StringBuilder sb = new StringBuilder(st.getBareName());
        var par = st.getParent();
        while (par != null && !(par instanceof IDatabase)) {
            sb.insert(0, '.').insert(0, par.getBareName());
            par = par.getParent();
        }

        return sb.toString();
    }

    private StatementUtils() {
    }
}
