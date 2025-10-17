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

import org.pgcodekeeper.core.Utils;
import org.pgcodekeeper.core.localizations.Messages;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Abstract base class for JDBC database connectors.
 * Provides common functionality for establishing database connections.
 */
public abstract class AbstractJdbcConnector {

    /**
     * Creates a new database connection using the parameters specified in the constructor.
     * The caller is responsible for closing the connection.
     *
     * @return new database connection
     * @throws IOException if the driver is not found or a database access error occurs
     */
    public Connection getConnection() throws IOException {
        try {
            loadDriver();
            return DriverManager.getConnection(getUrl(), makeProperties());
        } catch (SQLException | ClassNotFoundException e) {
            throw new IOException(e.getLocalizedMessage(), e);
        }
    }

    /**
     * Loads driver classes
     *
     * @throws ClassNotFoundException if driver classes are not found in classpath
     */
    protected void loadDriver() throws ClassNotFoundException {
        Class.forName(getDriverName());
    }

    /**
     * @return full connection string
     */
    protected abstract String getUrl();

    /**
     * @return connection properties
     */
    protected Properties makeProperties() {
        Properties props = new Properties();
        props.setProperty("ApplicationName", "pgCodeKeeper, version: " + Utils.getVersion());
        return props;
    }

    /**
     * @return driver name
     */
    protected abstract String getDriverName();

    /**
     * Returns batch delimiter. If the value is null, each statement will be executed separately in autocommit mode.
     *
     * @return batch delimiter
     */
    public String getBatchDelimiter() {
        return null;
    }

    protected abstract String getDefaultPort();

    protected void validateUrl(String url, String... allowedPrefixes) {
        for (var prefix : allowedPrefixes) {
            if (url.startsWith(prefix)) {
                return;
            }
        }

        var options = String.join(", ", allowedPrefixes);
        throw new IllegalArgumentException(Messages.AbstractJdbcConnector_url_validation_failed.formatted(options));
    }
}
