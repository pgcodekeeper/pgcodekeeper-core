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
package org.pgcodekeeper.core.loader.callables;

import org.pgcodekeeper.core.Consts;

import java.sql.PreparedStatement;
import java.sql.Statement;

/**
 * Executable callable for running single SQL queries.
 * Supports both prepared statements and regular statements with dynamic SQL scripts.
 * Returns a success constant upon completion.
 */
public class QueryCallable extends StatementCallable<String> {

    /**
     * Creates a new query callable with a prepared statement.
     *
     * @param st the prepared statement to execute
     */
    public QueryCallable(PreparedStatement st) {
        super(st, null);
    }

    /**
     * Creates a new query callable with a statement and SQL script.
     *
     * @param st     the statement to execute
     * @param script the SQL script to execute
     */
    public QueryCallable(Statement st, String script) {
        super(st, script);
    }

    @Override
    public String call() throws Exception {
        if (st instanceof PreparedStatement ps) {
            ps.execute();
        } else {
            st.execute(script);
        }
        return Consts.JDBC_SUCCESS;
    }
}
