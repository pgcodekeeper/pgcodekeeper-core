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
package org.pgcodekeeper.core.database.api.jdbc;

import java.io.IOException;
import java.sql.Connection;

public interface IJdbcConnector {

    /**
     * Creates a new database connection using the parameters specified in the constructor.
     * The caller is responsible for closing the connection.
     *
     * @return new database connection
     * @throws IOException if the driver is not found or a database access error occurs
     */
    Connection getConnection() throws IOException;

    /**
     * Returns batch delimiter. If the value is null, each statement will be executed separately in autocommit mode.
     *
     * @return batch delimiter
     */
    String getBatchDelimiter();

    /**
     * @return connection string
     */
    String getUrl();
}
