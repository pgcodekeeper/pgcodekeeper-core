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
package org.pgcodekeeper.core.database.base.jdbc;

import java.sql.*;

import org.pgcodekeeper.core.database.api.jdbc.IJdbcReader;
import org.pgcodekeeper.core.database.base.loader.AbstractJdbcLoader;
import org.pgcodekeeper.core.exception.XmlReaderException;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.monitor.IMonitor;

/**
 * Abstract base class for JDBC statement readers that process database metadata.
 * Provides common functionality for building SQL queries with extension and description support,
 * and processing database objects from ResultSets.
 */
public abstract class AbstractJdbcReader<T extends AbstractJdbcLoader> implements IJdbcReader {

    protected final T loader;

    protected AbstractJdbcReader(T loader) {
        this.loader = loader;
    }

    public void read() throws SQLException, InterruptedException, XmlReaderException {
        loader.setCurrentOperation(Messages.AbstractStatementReader_start + getClass().getSimpleName());
        QueryBuilder builder = makeQuery();
        if (builder == null) {
            return;
        }
        String query = builder.build();

        try (PreparedStatement statement = loader.getConnection().prepareStatement(query)) {
            setQueryParams(statement);
            ResultSet result = loader.getRunner().runScript(statement);
            IMonitor monitor = loader.getMonitor();
            while (result.next()) {
                IMonitor.checkCancelled(monitor);
                monitor.worked(1);
                processResult(result);
            }
        }
    }

    protected QueryBuilder makeQuery() {
        QueryBuilder builder = new QueryBuilder();
        fillQueryBuilder(builder);
        return builder;
    }

    protected abstract void fillQueryBuilder(QueryBuilder builder);

    /**
     * Processing {@link ResultSet} from implementation of database and create correct model
     *
     * @param result query result
     * @throws SQLException         if database access fails
     * @throws XmlReaderException   if XML processing fails
     */
    protected abstract void processResult(ResultSet result) throws SQLException, XmlReaderException;

    /**
     * Setter for specific parameters for implementation of Jdbc Reader
     *
     * @param statement instance of {@link PreparedStatement}
     * @throws SQLException if parameter is set incorrectly
     */
    protected void setQueryParams(PreparedStatement statement) throws SQLException {
        // do nothing by default
    }
}