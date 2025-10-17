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
package org.pgcodekeeper.core.database.ms.jdbc;

import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.database.base.jdbc.AbstractJdbcConnector;

import java.util.Properties;

/**
 * JDBC database connector implementation for MS SQL.
 */
public class MsJdbcConnector extends AbstractJdbcConnector {

    private static final String DRIVER_NAME = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

    protected static final String URL_START_MS = "jdbc:sqlserver:";

    private final String url;

    /**
     * @param url full jdbc connection string
     */
    public MsJdbcConnector(String url) {
        validateUrl(url, URL_START_MS);
        this.url = url;
    }

    @Override
    protected String getUrl() {
        return url;
    }

    @Override
    protected String getDriverName() {
        return DRIVER_NAME;
    }

    @Override
    protected Properties makeProperties() {
        Properties props = super.makeProperties();
        if (url != null && !url.contains(Consts.TRUST_CERT)) {
            // revert to pre-10.x jdbc driver behavior unless told otherwise
            props.setProperty(Consts.TRUST_CERT, "true");
        }
        return props;
    }

    @Override
    public String getBatchDelimiter() {
        return Consts.GO;
    }

    @Override
    protected String getDefaultPort() {
        return "1433";
    }
}
