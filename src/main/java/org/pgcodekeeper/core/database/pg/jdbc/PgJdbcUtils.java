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
package org.pgcodekeeper.core.database.pg.jdbc;

import java.sql.*;

import org.pgcodekeeper.core.localizations.Messages;

public final class PgJdbcUtils {

    public static <T> T[] getColArray(ResultSet rs, String columnName) throws SQLException {
        return getColArray(rs, columnName, false);
    }

    /**
     * Retrieves an array column from the result set.
     * Returns the array values if present, or handles null values based on the allowed null flag.
     *
     * @param <T> the array element type
     * @param rs the result set containing the data
     * @param columnName the name of the column containing the array
     * @param isAllowedNull if true, returns null when column value is null;
     *                      if false, throws IllegalArgumentException when column value is null
     * @return the array values from the specified column, or null if the column value is null and nulls are allowed
     * @throws SQLException if array retrieval from the result set fails
     * @throws IllegalArgumentException if the column value is null and nulls are not allowed
     */
    public static <T> T[] getColArray(ResultSet rs, String columnName, boolean isAllowedNull) throws SQLException {
        Array arr = rs.getArray(columnName);
        if (arr != null) {
            @SuppressWarnings("unchecked")
            T[] ret = (T[]) arr.getArray();
            return ret;
        }

        if (isAllowedNull) {
            return null;
        }
        String callerClassName = Thread.currentThread().getStackTrace()[2].getFileName();
        throw new IllegalArgumentException(Messages.JdbcReader_column_null_value_error_message.formatted(columnName, callerClassName));
    }
}
