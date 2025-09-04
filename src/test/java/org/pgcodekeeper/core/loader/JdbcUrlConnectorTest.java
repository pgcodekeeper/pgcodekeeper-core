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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;

class JdbcUrlConnectorTest {

    @Test
    void failedConnectionTest() {
        var connector = new UrlJdbcConnector("jdbc:postgresql://localhost:5432/broken");
        Assertions.assertThrows(IOException.class, connector::getConnection);
    }

    @Test
    void pgConnectionTest() {
        var connector = new UrlJdbcConnector(TestContainer.PG_URL);
        Assertions.assertDoesNotThrow(() -> testConnector(connector));
    }

    @Test
    void msConnectionTest() {
        var connector = new UrlJdbcConnector(TestContainer.MS_URL);
        Assertions.assertDoesNotThrow(() -> testConnector(connector));
    }

    @Test
    void chConnectionTest() {
        var connector = new UrlJdbcConnector(TestContainer.CH_URL);
        Assertions.assertDoesNotThrow(() -> testConnector(connector));
    }

    void testConnector(AbstractJdbcConnector connector) throws IOException, SQLException {
        try (var connection = connector.getConnection();
             var statement = connection.createStatement();
             var rs = statement.executeQuery("SELECT 1")) {
            Assertions.assertTrue(rs.next());
        }
    }
}