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

import com.microsoft.sqlserver.jdbc.SQLServerError;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.reporter.IProgressReporter;
import org.pgcodekeeper.core.loader.jdbc.JdbcType;
import org.pgcodekeeper.core.database.api.schema.ObjectLocation;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Executable callable for processing batches of SQL queries with progress monitoring and error reporting.
 * Handles database-specific batch execution for PostgreSQL, ClickHouse, and Microsoft SQL databases.
 * Provides detailed error reporting with position information and supports both single statements and batch operations.
 */
public class QueriesBatchCallable extends StatementCallable<String> {

    private final List<ObjectLocation> batches;
    private final IMonitor monitor;
    private final Connection connection;
    private final IProgressReporter reporter;
    private final String batchDelimiter;

    private boolean isAutoCommitEnabled = true;

    /**
     * Creates a new queries batch callable with the specified parameters.
     *
     * @param st             the SQL statement to execute
     * @param batches        list of SQL query locations to process in batch
     * @param monitor        progress monitor for tracking execution progress
     * @param reporter       progress reporter for writing execution results and errors
     * @param connection     database connection for batch operations
     * @param batchDelimiter batch delimiter for split
     */
    public QueriesBatchCallable(Statement st, List<ObjectLocation> batches,
                                IMonitor monitor, IProgressReporter reporter,
                                Connection connection, String batchDelimiter) {
        super(st, null);
        this.batches = batches;
        this.monitor = monitor;
        this.connection = connection;
        this.reporter = reporter;
        this.batchDelimiter = batchDelimiter;
    }

    @Override
    public String call() throws Exception {
        IMonitor subMonitor = monitor.createSubMonitor();
        ObjectLocation currQuery = null;
        String[] finalModifiedQuery = new String[1];

        try {
            if (reporter != null) {
                reporter.writeDbName();
            }

            List<List<ObjectLocation>> batchesList = getListBatchesFromSetBatches();
            subMonitor.setWorkRemaining(batchesList.size());
            for (List<ObjectLocation> queriesList : batchesList) {
                IMonitor.checkCancelled(monitor);
                // in case we're executing a real batch after a single-statement one
                currQuery = null;
                if (queriesList.size() == 1) {
                    currQuery = queriesList.get(0);
                    executeSingleStatement(currQuery, finalModifiedQuery);
                } else {
                    runBatch(queriesList);
                }
                subMonitor.worked(1);
            }
            if (!isAutoCommitEnabled) {
                connection.commit();
            }

            if (reporter != null) {
                reporter.writeMessage("Script finished"); //$NON-NLS-1$
            }
        } catch (PSQLException ex) {
            ServerErrorMessage sem = ex.getServerErrorMessage();
            if (reporter == null || sem == null) {
                throw ex;
            }
            StringBuilder sb = new StringBuilder(sem.toString());
            if (currQuery != null) {
                int offset = sem.getPosition();
                if (offset > 0) {
                    appendPosition(sb, finalModifiedQuery[0], offset);
                } else {
                    if (currQuery.getLineNumber() > 1) {
                        sb.append("\n  Line: ").append(currQuery.getLineNumber()); //$NON-NLS-1$
                    }
                    sb.append('\n').append(currQuery.getSql());
                }
                reporter.reportErrorLocation(currQuery.getOffset(),
                        currQuery.getSql().length());
            }

            reporter.writeError(sb.toString());
        } catch (SQLServerException e) {
            SQLServerError err = e.getSQLServerError();
            if (reporter == null || err == null) {
                throw e;
            }
            StringBuilder sb = new StringBuilder(err.getErrorMessage());
            if (currQuery != null) {
                if (err.getLineNumber() > 1) {
                    sb.append("\n  Line: ").append(err.getLineNumber()); //$NON-NLS-1$
                } else if (currQuery.getLineNumber() > 1) {
                    sb.append("\n  Line: ").append(currQuery.getLineNumber()); //$NON-NLS-1$
                }
                sb.append('\n').append(currQuery.getSql());
                reporter.reportErrorLocation(currQuery.getOffset(),
                        currQuery.getSql().length());
            }

            reporter.writeError(sb.toString());
        }
        // BatchUpdateException
        // MS SQL driver returns a useless batch update status array
        // where even successful statements are marked as Statement.EXECUTE_FAILED
        // so we cannot deduce which one failed to show more accurate error context

        return Consts.JDBC_SUCCESS;
    }

    private List<List<ObjectLocation>> getListBatchesFromSetBatches() {
        List<List<ObjectLocation>> batchesList = new ArrayList<>();
        List<ObjectLocation> currentBatch = new ArrayList<>();

        for (ObjectLocation loc : batches) {
            if (null == batchDelimiter) {
                currentBatch.add(loc);
                batchesList.add(currentBatch);
                currentBatch = new ArrayList<>();
            } else if (batchDelimiter.equalsIgnoreCase(loc.getAction())) {
                batchesList.add(currentBatch);
                currentBatch = new ArrayList<>();
            } else {
                currentBatch.add(loc);
            }
        }

        if (!currentBatch.isEmpty()) {
            batchesList.add(currentBatch);
        }

        return batchesList;
    }

    private void executeSingleStatement(ObjectLocation query, String[] finalModifiedQuery)
            throws SQLException, InterruptedException {
        String sql = query.getSql();

        finalModifiedQuery[0] = sql;
        if (st.execute(sql)) {
            writeResult(query.getSql());
        }
        writeWarnings();
        writeStatus(query.getAction());
    }

    private void runBatch(List<ObjectLocation> queriesList)
            throws SQLException {
        if (isAutoCommitEnabled) {
            connection.setAutoCommit(false);
            isAutoCommitEnabled = false;
        }

        if (reporter != null) {
            reporter.writeMessage("Starting batch"); //$NON-NLS-1$
        }

        for (ObjectLocation query : queriesList) {
            st.addBatch(query.getSql());
            writeStatus(query.getAction());
        }

        if (reporter != null) {
            reporter.writeMessage("Executing batch"); //$NON-NLS-1$
        }

        st.executeBatch();
        writeWarnings();
    }

    private void writeResult(String query) throws SQLException, InterruptedException {
        if (reporter == null) {
            return;
        }
        List<List<Object>> results = new ArrayList<>();
        try (ResultSet res = st.getResultSet()) {

            ResultSetMetaData meta = res.getMetaData();
            int count = meta.getColumnCount();

            // add column names as first list
            List<Object> names = new ArrayList<>(count);
            for (int i = 1; i <= count; i++) {
                String type = meta.getColumnTypeName(i);
                String dealias = JdbcType.DATA_TYPE_ALIASES.get(type);
                names.add(meta.getColumnLabel(i) + ' ' +
                        (dealias == null ? type : dealias));
            }
            results.add(names);

            // add other rows
            while (res.next()) {
                IMonitor.checkCancelled(monitor);
                List<Object> row = new ArrayList<>(count);
                results.add(row);
                for (int i = 1; i <= count; i++) {
                    row.add(res.getObject(i));
                }
            }
        }
        reporter.showData(query, results);
    }

    private void writeWarnings() throws SQLException {
        if (reporter == null) {
            return;
        }

        SQLWarning war = st.getWarnings();
        while (war != null) {
            reporter.writeWarning(war.getLocalizedMessage());
            war = war.getNextWarning();
        }
    }

    private void writeStatus(String msgAction) {
        if (reporter == null || msgAction == null) {
            return;
        }
        reporter.writeMessage(msgAction);
    }

    private void appendPosition(StringBuilder sb, String query, int offset) {
        sb.append('\n');
        int begin = query.lastIndexOf('\n', offset) + 1;
        int end = query.indexOf('\n', offset);

        if (end == offset) {
            // error in last character in previous line
            begin = query.lastIndexOf('\n', offset - 1) + 1;
        }

        String line;
        if (end == -1) {
            line = query.substring(begin);
        } else {
            line = query.substring(begin, end);
        }

        sb.append(line.replace("\t", "    ")).append('\n'); //$NON-NLS-1$ //$NON-NLS-2$

        for (int i = 0; i < offset - begin - 1; i++) {
            if ('\t' == line.charAt(i)) {
                sb.append("    "); //$NON-NLS-1$
            } else {
                sb.append(' ');
            }
        }
        sb.append('^');
    }
}
