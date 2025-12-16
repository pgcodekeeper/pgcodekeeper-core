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

import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.Utils;
import org.pgcodekeeper.core.database.api.jdbc.IJdbcConnector;
import org.pgcodekeeper.core.database.ch.loader.JdbcChLoader;
import org.pgcodekeeper.core.database.ms.loader.JdbcMsLoader;
import org.pgcodekeeper.core.database.pg.loader.JdbcPgLoader;
import org.pgcodekeeper.core.ignorelist.IgnoreSchemaList;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.monitor.NullMonitor;

/**
 * Factory class for creating database loaders based on database type.
 */
public final class LoaderFactory {

    /**
     * Creates a JDBC database loader using URL-based connection.
     *
     * @param settings         loader settings and configuration
     * @param url              the JDBC URL for database connection
     * @param ignoreSchemaList list of schemas to ignore during loading
     * @return database loader for the specified database type
     */
    public static DatabaseLoader createJdbcLoader(ISettings settings, String url,
                                                  IgnoreSchemaList ignoreSchemaList) {
        return createJdbcLoader(settings, url, ignoreSchemaList, new NullMonitor());
    }

    /**
     * Creates a JDBC database loader using URL-based connection and monitoring.
     *
     * @param settings         loader settings and configuration
     * @param url              the JDBC URL for database connection
     * @param ignoreSchemaList list of schemas to ignore during loading
     * @param monitor          progress monitor for tracking the operation
     * @return database loader for the specified database type
     */
    public static DatabaseLoader createJdbcLoader(ISettings settings, String url,
                                                  IgnoreSchemaList ignoreSchemaList, IMonitor monitor) {
        String timezone = settings.getTimeZone() == null ? Consts.UTC : settings.getTimeZone();
        return createJdbcLoader(settings, timezone, Utils.getJdbcConnectorByType(settings.getDbType(), url), monitor,
                ignoreSchemaList);
    }

    /**
     * Creates a JDBC database loader with specified connector and monitoring.
     *
     * @param settings         settings file
     * @param timezone         timezone setting for PostgreSQL connections
     * @param connector        the JDBC connector for database connection
     * @param monitor          progress monitor for tracking loading progress
     * @param ignoreSchemaList list of schemas to ignore during loading
     * @return database loader for the specified database type
     */
    public static DatabaseLoader createJdbcLoader(ISettings settings, String timezone,
                                                  IJdbcConnector connector, IMonitor monitor, IgnoreSchemaList ignoreSchemaList) {
        return switch (settings.getDbType()) {
            case MS -> new JdbcMsLoader(connector, settings, monitor, ignoreSchemaList);
            case PG -> new JdbcPgLoader(connector, timezone, settings, monitor, ignoreSchemaList);
            case CH -> new JdbcChLoader(connector, settings, monitor, ignoreSchemaList);
        };
    }

    /**
     * Private constructor to prevent instantiation of factory class.
     */
    private LoaderFactory() {
    }
}