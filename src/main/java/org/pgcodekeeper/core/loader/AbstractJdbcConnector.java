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

import org.pgcodekeeper.core.DatabaseType;
import org.pgcodekeeper.core.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Abstract base class for JDBC database connectors.
 * Provides common functionality for establishing database connections across different database types
 * including PostgreSQL, Microsoft SQL Server, and ClickHouse.
 */
public abstract class AbstractJdbcConnector {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractJdbcConnector.class);

    private static final String PG_DRIVER_NAME = "org.postgresql.Driver";
    private static final String MS_DRIVER_NAME = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private static final String CH_DRIVER_NAME = "com.clickhouse.jdbc.ClickHouseDriver";

    protected static final String URL_START_MS = "jdbc:sqlserver:";
    protected static final String URL_START_PG = "jdbc:postgresql:";
    protected static final String URL_START_CH = "jdbc:clickhouse:";
    protected static final String URL_START_CH_SHORT = "jdbc:ch:";

    protected DatabaseType dbType;

    protected AbstractJdbcConnector(DatabaseType dbType) {
        this.dbType = dbType;
    }

    /**
     * Creates a new database connection using the parameters specified in the constructor.
     * The caller is responsible for closing the connection.
     *
     * @return new database connection
     * @throws IOException if the driver is not found or a database access error occurs
     */
    public Connection getConnection() throws IOException {
        try {
            Class.forName(getDriverName());
            return DriverManager.getConnection(getUrl(), makeProperties());
        } catch (SQLException | ClassNotFoundException e) {
            throw new IOException(e.getLocalizedMessage(), e);
        }
    }

    protected abstract String getUrl();

    protected Properties makeProperties() {
        Properties props = new Properties();
        var coreVer = Utils.getVersion();
        props.setProperty("ApplicationName", "pgCodeKeeper, version: " + coreVer);
        return props;
    }

    protected String getDriverName() {
        return switch (dbType) {
            case PG -> PG_DRIVER_NAME;
            case MS -> MS_DRIVER_NAME;
            case CH -> CH_DRIVER_NAME;
        };
    }

    public DatabaseType getType() {
        return dbType;
    }

    protected void log(String message) {
        if (message != null) {
            LOG.info(message);
        }
    }
}