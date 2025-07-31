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
package org.pgcodekeeper.core.loader;

import org.pgcodekeeper.core.DaemonThreadFactory;
import org.pgcodekeeper.core.IProgressReporter;
import org.pgcodekeeper.core.loader.callables.QueriesBatchCallable;
import org.pgcodekeeper.core.loader.callables.QueryCallable;
import org.pgcodekeeper.core.loader.callables.ResultSetCallable;
import org.pgcodekeeper.core.loader.callables.StatementCallable;
import org.pgcodekeeper.core.schema.PgObjLocation;
import org.pgcodekeeper.core.utils.IMonitor;
import org.pgcodekeeper.core.utils.NullMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.concurrent.*;

/**
 * JDBC statement execution runner with progress monitoring and cancellation support.
 * Provides methods for executing SQL statements, prepared statements, and batch operations
 * with progress tracking and interrupt handling.
 */
public class JdbcRunner {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcRunner.class);

    private static final ExecutorService THREAD_POOL = new ThreadPoolExecutor(1,
            Integer.MAX_VALUE, 2, TimeUnit.SECONDS, new SynchronousQueue<>(),
            new DaemonThreadFactory());

    private static final int SLEEP_TIME = 20;

    private static final String MESSAGE = "Script execution interrupted by user";

    private final IMonitor monitor;


    /**
     * Creates a new JDBC runner with a null progress monitor.
     */
    public JdbcRunner() {
        this(new NullMonitor());
    }

    /**
     * Creates a new JDBC runner with the specified progress monitor.
     *
     * @param monitor the progress monitor for tracking execution and handling cancellation
     */
    public JdbcRunner(IMonitor monitor) {
        this.monitor = monitor;
    }

    /**
     * Executes a prepared statement with no return value.
     *
     * @param st the prepared statement to execute
     * @throws SQLException         if a database access error occurs
     * @throws InterruptedException if execution is interrupted
     */
    public void run(PreparedStatement st) throws SQLException, InterruptedException {
        runScript(new QueryCallable(st));
    }

    /**
     * Executes a statement using the given script with no return value.
     *
     * @param st     the statement to execute
     * @param script the SQL script to execute
     * @throws SQLException         if a database access error occurs
     * @throws InterruptedException if execution is interrupted
     */
    public void run(Statement st, String script) throws SQLException, InterruptedException {
        runScript(new QueryCallable(st, script));
    }

    /**
     * Executes a script using a new connection from the connector.
     *
     * @param connector the JDBC connector for database connection
     * @param script    the SQL script to execute
     * @throws SQLException         if a database access error occurs
     * @throws IOException          if connection creation fails
     * @throws InterruptedException if execution is interrupted
     */
    public void run(AbstractJdbcConnector connector, String script)
            throws SQLException, IOException, InterruptedException {
        try (Connection connection = connector.getConnection();
             Statement st = connection.createStatement()) {
            run(st, script);
        }
    }

    /**
     * Executes statement batches with no return value.
     *
     * @param connector database connection information
     * @param batches   list of query batches to execute
     * @param reporter  progress and result reporter
     * @throws SQLException         if a database access error occurs
     * @throws IOException          if connection creation fails
     * @throws InterruptedException if execution is interrupted
     */
    public void runBatches(AbstractJdbcConnector connector, List<PgObjLocation> batches,
                           IProgressReporter reporter) throws SQLException, IOException, InterruptedException {
        try (Connection connection = connector.getConnection();
             Statement st = connection.createStatement()) {
            runScript(new QueriesBatchCallable(st, batches, monitor, reporter, connection, connector.getType()));
        }
    }

    /**
     * Executes a prepared statement and returns the result set.
     *
     * @param st the prepared statement to execute
     * @return the result set from the query
     * @throws SQLException         if a database access error occurs
     * @throws InterruptedException if execution is interrupted
     */
    public ResultSet runScript(PreparedStatement st) throws InterruptedException, SQLException {
        return runScript(new ResultSetCallable(st));
    }

    /**
     * Executes a statement using the given script and returns the result set.
     *
     * @param st     the statement to execute
     * @param script the SQL script to execute
     * @return the result set from the query
     * @throws SQLException         if a database access error occurs
     * @throws InterruptedException if execution is interrupted
     */
    public ResultSet runScript(Statement st, String script) throws InterruptedException, SQLException {
        return runScript(new ResultSetCallable(st, script));
    }

    private <T> T runScript(StatementCallable<T> callable) throws InterruptedException, SQLException {
        Future<T> queryFuture = THREAD_POOL.submit(callable);

        while (true) {
            if (monitor.isCanceled()) {
                LOG.info(MESSAGE);
                callable.cancel();
                throw new InterruptedException(MESSAGE);
            }
            try {
                return queryFuture.get(SLEEP_TIME, TimeUnit.MILLISECONDS);
            } catch (ExecutionException e) {
                Throwable t = e.getCause();
                throw new SQLException(t.getLocalizedMessage(), e);
            } catch (TimeoutException e) {
                // no action: check cancellation and try again
            }
        }
    }
}
