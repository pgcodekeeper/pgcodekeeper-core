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
package org.pgcodekeeper.core.callable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Executable callable for running SQL queries that return result sets.
 * Supports both prepared statements and regular statements with dynamic SQL scripts.
 * Returns the ResultSet obtained from query execution.
 */
public class ResultSetCallable extends StatementCallable<ResultSet> {

    /**
     * Creates a new result set callable with a prepared statement.
     *
     * @param st the prepared statement to execute
     */
    public ResultSetCallable(PreparedStatement st) {
        super(st, null);
    }

    /**
     * Creates a new result set callable with a statement and SQL script.
     *
     * @param st     the statement to execute
     * @param script the SQL script to execute
     */
    public ResultSetCallable(Statement st, String script) {
        super(st, script);
    }

    @Override
    public ResultSet call() throws Exception {
        if (st instanceof PreparedStatement ps) {
            return ps.executeQuery();
        }

        return st.executeQuery(script);
    }
}
