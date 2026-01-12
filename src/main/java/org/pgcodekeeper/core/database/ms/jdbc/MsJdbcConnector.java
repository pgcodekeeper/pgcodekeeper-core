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
package org.pgcodekeeper.core.database.ms.jdbc;

import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.database.base.jdbc.AbstractJdbcConnector;

import java.sql.SQLException;
import java.util.Properties;

/**
 * JDBC database connector implementation for MS SQL.
 */
public class MsJdbcConnector extends AbstractJdbcConnector {

    private static final String URL_START_MS = "jdbc:sqlserver:";

    private static final int DEFAULT_PORT = 1433;

    /**
     * @param url full jdbc connection string
     */
    public MsJdbcConnector(String url) {
        super(url);
        validateUrl(url, URL_START_MS);
    }

    public MsJdbcConnector(String host, int port, String dbName) {
        super(URL_START_MS + "//" + host + ':' + (port > 0 ? port : DEFAULT_PORT)
                + (dbName == null ? "" : ";databaseName={" + dbName + '}'));
    }

    @Override
    protected void loadDriver() throws SQLException {
        SQLServerDriver.register();
    }

    @Override
    protected Properties makeProperties() {
        Properties props = super.makeProperties();
        if (!getUrl().contains(Consts.TRUST_CERT)) {
            // revert to pre-10.x jdbc driver behavior unless told otherwise
            props.setProperty(Consts.TRUST_CERT, "true");
        }
        return props;
    }

    @Override
    public String getBatchDelimiter() {
        return Consts.GO;
    }
}
