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

import org.eclipse.core.runtime.SubMonitor;
import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.loader.ch.JdbcChLoader;
import org.pgcodekeeper.core.loader.ms.JdbcMsLoader;
import org.pgcodekeeper.core.loader.pg.JdbcPgLoader;
import org.pgcodekeeper.core.model.difftree.IgnoreSchemaList;
import org.pgcodekeeper.core.settings.ISettings;

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
        String timezone = settings.getTimeZone() == null ? Consts.UTC : settings.getTimeZone();
        return createJdbcLoader(settings, timezone, new UrlJdbcConnector(url), SubMonitor.convert(null),
                ignoreSchemaList);
    }

    /**
     * Creates a JDBC database loader with specified connector and monitoring.
     *
     * @param settings         settings file
     * @param timezone         timezone setting for PostgreSQL connections
     * @param connnector       the JDBC connector for database connection
     * @param monitor          progress monitor for tracking loading progress
     * @param ignoreSchemaList list of schemas to ignore during loading
     * @return database loader for the specified database type
     */
    public static DatabaseLoader createJdbcLoader(ISettings settings, String timezone,
                                                  AbstractJdbcConnector connnector, SubMonitor monitor, IgnoreSchemaList ignoreSchemaList) {
        return switch (settings.getDbType()) {
            case MS -> new JdbcMsLoader(connnector, settings, monitor, ignoreSchemaList);
            case PG -> new JdbcPgLoader(connnector, timezone, settings, monitor, ignoreSchemaList);
            case CH -> new JdbcChLoader(connnector, settings, monitor, ignoreSchemaList);
        };
    }

    /**
     * Private constructor to prevent instantiation of factory class.
     */
    private LoaderFactory() {
    }
}