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

import org.pgcodekeeper.core.utils.Utils;
import org.pgcodekeeper.core.database.api.jdbc.IJdbcConnector;
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
public abstract class AbstractJdbcConnector implements IJdbcConnector {

    private final String url;

    /**
     * @param url jdbc connection string
     */
    protected AbstractJdbcConnector(String url) {
        this.url = url;
    }

    @Override
    public Connection getConnection() throws IOException {
        try {
            loadDriver();
            return DriverManager.getConnection(getUrl(), makeProperties());
        } catch (SQLException e) {
            throw new IOException(e.getLocalizedMessage(), e);
        }
    }

    /**
     * Loads driver classes
     *
     * @throws SQLException if driver already load or something gone are wrong
     */
    protected abstract void loadDriver() throws SQLException;

    @Override
    public String getUrl() {
        return url;
    }

    /**
     * @return connection properties
     */
    protected Properties makeProperties() {
        Properties props = new Properties();
        props.setProperty("ApplicationName", "pgCodeKeeper, version: " + Utils.getVersion());
        return props;
    }

    /**
     * Validates connection string
     *
     * @param url connection string
     * @param allowedPrefixes allowed prefixes
     * @throws IllegalArgumentException if the string does not start with the allowed prefixes
     */
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
