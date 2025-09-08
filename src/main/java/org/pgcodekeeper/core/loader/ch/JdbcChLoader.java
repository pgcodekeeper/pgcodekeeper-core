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
package org.pgcodekeeper.core.loader.ch;

import org.pgcodekeeper.core.PgDiffUtils;
import org.pgcodekeeper.core.database.base.jdbc.AbstractJdbcConnector;
import org.pgcodekeeper.core.loader.jdbc.JdbcLoaderBase;
import org.pgcodekeeper.core.loader.jdbc.ch.*;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.model.difftree.IgnoreSchemaList;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.schema.AbstractDatabase;
import org.pgcodekeeper.core.schema.ch.ChDatabase;
import org.pgcodekeeper.core.settings.ISettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.stream.Collectors;

/**
 * JDBC-based database schema loader for ClickHouse databases.
 * Reads database schemas, functions, relations, policies, users, roles, and privileges from a ClickHouse database.
 * Extends JdbcLoaderBase to provide ClickHouse-specific loading functionality.
 */
public final class JdbcChLoader extends JdbcLoaderBase {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcChLoader.class);

    /**
     * Creates a new ClickHouse JDBC loader with the specified parameters.
     *
     * @param connector        the JDBC connector for establishing database connections
     * @param settings         loader settings and configuration
     * @param monitor          progress monitor for tracking loading progress
     * @param ignoreSchemaList list of schemas to ignore during loading
     */
    public JdbcChLoader(AbstractJdbcConnector connector, ISettings settings, IMonitor monitor,
                        IgnoreSchemaList ignoreSchemaList) {
        super(connector, monitor, settings, ignoreSchemaList);
    }

    @Override
    public AbstractDatabase load() throws IOException, InterruptedException {
        ChDatabase d = (ChDatabase) createDb(getSettings());

        LOG.info(Messages.JdbcLoader_log_reading_db_jdbc);
        setCurrentOperation(Messages.JdbcChLoader_log_connection_db);
        try (Connection connection = connector.getConnection();
             Statement statement = connection.createStatement()) {
            this.connection = connection;
            this.statement = statement;

            connection.setAutoCommit(false);

            LOG.info(Messages.JdbcLoader_log_read_db_objects);
            new ChSchemasReader(this, d).read();
            new ChFunctionsReader(this, d).read();
            new ChRelationsReader(this).read();
            new ChPoliciesReader(this, d).read();
            new ChUsersReader(this, d).read();
            new ChRolesReader(this, d).read();
            if (!getSettings().isIgnorePrivileges()) {
                new ChPrivilegesReader(this, d).read();
            }

            finishLoaders();
            connection.commit();

            LOG.info(Messages.JdbcLoader_log_succes_queried);
        } catch (InterruptedException ex) {
            throw ex;
        } catch (Exception e) {
            // connection is closed at this point
            throw new IOException(Messages.Connection_DatabaseJdbcAccessError.formatted(getCurrentLocation(),
                    e.getLocalizedMessage()), e);
        }
        return d;
    }

    @Override
    public String getSchemas() {
        return schemaIds.keySet().stream()
                .map(e -> PgDiffUtils.quoteString(e.toString()))
                .collect(Collectors.joining(", ")); //$NON-NLS-1$
    }
}
