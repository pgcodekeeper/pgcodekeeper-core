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
package org.pgcodekeeper.core.database.pg.loader;

import org.pgcodekeeper.core.database.api.jdbc.IJdbcConnector;
import org.pgcodekeeper.core.database.pg.loader.jdbc.*;
import org.pgcodekeeper.core.loader.JdbcQueries;
import org.pgcodekeeper.core.loader.jdbc.JdbcLoaderBase;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.ignorelist.IgnoreSchemaList;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.database.pg.schema.PgDatabase;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * JDBC-based database schema loader for PostgreSQL databases.
 * Reads database schemas, functions, views, tables, types, sequences, extensions, and other objects from a PostgreSQL database.
 * Supports timezone configuration and Greenplum database detection.
 * Extends JdbcLoaderBase to provide PostgreSQL-specific loading functionality.
 */
public final class JdbcPgLoader extends JdbcLoaderBase {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcPgLoader.class);
    private final String timezone;

    /**
     * Creates a new PostgreSQL JDBC loader with the specified parameters.
     *
     * @param connector        the JDBC connector for establishing database connections
     * @param timezone         the timezone to set for the database connection
     * @param settings         loader settings and configuration
     * @param monitor          progress monitor for tracking loading progress
     * @param ignoreSchemaList list of schemas to ignore during loading
     */
    public JdbcPgLoader(IJdbcConnector connector, String timezone, ISettings settings,
                        IMonitor monitor, IgnoreSchemaList ignoreSchemaList) {
        super(connector, monitor, settings, ignoreSchemaList);
        this.timezone = timezone;
    }

    @Override
    public PgDatabase load() throws IOException, InterruptedException {
        PgDatabase d = (PgDatabase) createDb(getSettings());

        LOG.info(Messages.JdbcLoader_log_reading_db_jdbc);
        setCurrentOperation(Messages.JdbcChLoader_log_connection_db);
        try (Connection connection = connector.getConnection();
             Statement statement = connection.createStatement()) {
            this.connection = connection;
            this.statement = statement;
            connection.setAutoCommit(false);
            getRunner().run(statement, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY");
            getRunner().run(statement, "SET search_path TO pg_catalog;");
            getRunner().run(statement, "SET timezone = " + Utils.quoteString(timezone));

            queryCheckGreenplumDb();
            queryCheckPgVersion();
            queryCheckLastSysOid();
            queryTypesForCache();
            queryRoles();
            queryCheckExtension();
            setupMonitorWork();

            LOG.info(Messages.JdbcLoader_log_read_db_objects);
            new PgSchemasReader(this, d).read();

            // NOTE: order of readers has been changed to move the heaviest ANTLR tasks to the beginning
            // to give them a chance to finish while JDBC processes other non-ANTLR stuff
            new PgFunctionsReader(this).read();
            new PgViewsReader(this).read();
            new PgTablesReader(this).read();
            new PgRulesReader(this).read();
            if (SupportedPgVersion.GP_VERSION_7.isLE(getVersion())) {
                new PgPoliciesReader(this).read();
            }
            new PgTriggersReader(this).read();
            new PgIndicesReader(this).read();
            new PgConstraintsReader(this).read();
            new PgTypesReader(this).read();
            if (SupportedPgVersion.GP_VERSION_7.isLE(getVersion())) {
                new PgStatisticsReader(this).read();
            }

            // non-ANTLR tasks
            new PgSequencesReader(this).read();
            new PgFtsParsersReader(this).read();
            new PgFtsTemplatesReader(this).read();
            new PgFtsDictionariesReader(this).read();
            new PgFtsConfigurationsReader(this).read();
            new PgOperatorsReader(this).read();

            new PgExtensionsReader(this, d).read();
            new PgEventTriggersReader(this, d).read();
            new PgCastsReader(this, d).read();
            new PgForeignDataWrappersReader(this, d).read();
            new PgServersReader(this, d).read();
            try (ResultSet res = getRunner().runScript(statement, JdbcQueries.QUERY_CHECK_USER_PRIVILEGES)) {
                if (res.next() && res.getBoolean("result")) {
                    new PgUserMappingsReader(this, d).read();
                }
            }
            new PgCollationsReader(this).read();

            if (!SupportedPgVersion.GP_VERSION_7.isLE(getVersion())) {
                PgSequencesReader.querySequencesData(d, this);
            }
            connection.commit();
            finishLoaders();

            d.sortColumns();

            d.setVersion(SupportedPgVersion.valueOf(getVersion()));
            LOG.info(Messages.JdbcLoader_log_succes_queried);
        } catch (InterruptedException ex) {
            throw ex;
        } catch (Exception e) {
            // connection is closed at this point, trust Postgres to rollback it; we're a read-only xact anyway
            throw new IOException(Messages.Connection_DatabaseJdbcAccessError.formatted(getCurrentLocation(),
                    e.getLocalizedMessage()), e);
        }
        return d;
    }
}
