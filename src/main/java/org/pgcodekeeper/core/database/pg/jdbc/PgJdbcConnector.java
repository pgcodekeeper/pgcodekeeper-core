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
package org.pgcodekeeper.core.database.pg.jdbc;

import org.pgcodekeeper.core.database.base.jdbc.AbstractJdbcConnector;

/**
 *  JDBC database connector implementation for PostgreSQL.
 */
public class PgJdbcConnector extends AbstractJdbcConnector {

    private static final String DRIVER_NAME = "org.postgresql.Driver";

    private static final String URL_START_PG = "jdbc:postgresql:";

    private static final int DEFAULT_PORT = 5432;

    /**
     * @param url full jdbc connection string
     */
    public PgJdbcConnector(String url) {
        super(url);
        validateUrl(url, URL_START_PG);
    }

    public PgJdbcConnector(String host, int port, String dbName) {
        super(URL_START_PG + "//" + host + ':' + (port > 0 ? port : DEFAULT_PORT) + '/' + dbName);
    }

    @Override
    protected String getDriverName() {
        return DRIVER_NAME;
    }

    @Override
    protected int getDefaultPort() {
        return DEFAULT_PORT;
    }

    @Override
    public String getBatchDelimiter() {
        return null;
    }
}
