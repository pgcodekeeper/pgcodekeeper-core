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
package org.pgcodekeeper.core.database.base.jdbc;

import org.pgcodekeeper.core.database.api.jdbc.IJdbcReader;
import org.pgcodekeeper.core.database.base.loader.AbstractJdbcLoader;
import org.pgcodekeeper.core.database.base.schema.AbstractSchema;
import org.pgcodekeeper.core.exception.ConcurrentModificationException;
import org.pgcodekeeper.core.exception.XmlReaderException;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class AbstractSearchPathJdbcReader<T extends AbstractJdbcLoader> implements IJdbcReader {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractSearchPathJdbcReader.class);

    protected final T loader;

    protected AbstractSearchPathJdbcReader(T loader) {
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

    private void processResult(ResultSet result) throws SQLException, XmlReaderException {
        String schemaColumn = getSchemaColumn();
        var schemaId = result.getObject(schemaColumn.substring(schemaColumn.indexOf('.') + 1));
        AbstractSchema schema = loader.getSchema(schemaId);
        if (schema == null) {
            var msg = "No schema found for id %s".formatted(schemaId);
            LOG.warn(msg);
            return;
        }

        try {
            processResult(result, schema);
        } catch (ConcurrentModificationException ex) {
            if (!loader.getSettings().isIgnoreConcurrentModification()) {
                throw ex;
            }
            LOG.error(ex.getLocalizedMessage(), ex);
        }
    }

    protected QueryBuilder makeQuery() {
        String schemas = loader.getSchemas();
        if (schemas.isBlank()) {
            return null;
        }
        QueryBuilder builder = new QueryBuilder();
        fillQueryBuilder(builder);
        builder.column(getSchemaColumn());
        builder.where(getSchemaColumn() + " IN (" + schemas + ')');
        return builder;
    }

    protected abstract void fillQueryBuilder(QueryBuilder builder);
    protected abstract String getSchemaColumn();
    protected abstract void processResult(ResultSet result, AbstractSchema schema)
            throws ConcurrentModificationException, SQLException, XmlReaderException;

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