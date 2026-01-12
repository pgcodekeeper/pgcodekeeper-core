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
package org.pgcodekeeper.core.loader.callables;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;

/**
 * Abstract base class for all statement-based callable implementations.
 * Provides common functionality for managing SQL statements and scripts in concurrent execution.
 *
 * @param <T> the return type of the callable execution
 */
public abstract class StatementCallable<T> implements Callable<T> {

    protected final Statement st;
    protected final String script;

    protected StatementCallable(Statement st, String script) {
        this.st = st;
        this.script = script;
    }

    /**
     * Cancels the SQL statement execution.
     *
     * @throws SQLException if a database access error occurs
     */
    public void cancel() throws SQLException {
        st.cancel();
    }
}
