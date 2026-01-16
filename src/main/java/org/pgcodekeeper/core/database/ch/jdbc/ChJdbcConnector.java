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
package org.pgcodekeeper.core.database.ch.jdbc;

import java.io.IOException;
import java.sql.*;

import org.pgcodekeeper.core.database.base.jdbc.AbstractJdbcConnector;

import com.clickhouse.jdbc.Driver;

/**
 * JDBC database connector implementation for ClickHouse.
 */
public class ChJdbcConnector extends AbstractJdbcConnector {

    private static final String URL_START_CH = "jdbc:clickhouse:";
    private static final String URL_START_CH_SHORT = "jdbc:ch:";

    private static final int DEFAULT_PORT = 8123;

    /**
     * @param url full jdbc connection string
     */
    public ChJdbcConnector(String url) {
        super(url);
        validateUrl(url, URL_START_CH, URL_START_CH_SHORT);
    }

    public ChJdbcConnector(String host, int port, String dbName) {
        super(URL_START_CH + "//" + host + ':' + (port > 0 ? port : DEFAULT_PORT) + (dbName == null ? "" : "/" + dbName));
    }

    @Override
    public Connection getConnection() throws IOException {
        var con = super.getConnection();
        //FIXME when the connection() method of the clickhouse driver throws an exception
        // when trying to connect to a non-existent database.
        try (var st = con.createStatement();
             var rs = st.executeQuery("SELECT 1")) {
            // check connection catch and throw exception if false
        } catch (SQLException e) {
            throw new IOException(e);
        }
        return con;
    }

    @Override
    protected void loadDriver() {
        Driver.load();
    }

    @Override
    public String getBatchDelimiter() {
        return null;
    }
}
