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

import java.io.IOException;
import java.sql.SQLException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.pgcodekeeper.core.loader.TestContainer;

class MsJdbcConnectorTest {

    @Test
    void msConnectionTest() throws IOException, SQLException {
        var connector = new MsJdbcConnector(TestContainer.MS_URL);
        try (var connection = connector.getConnection();
             var statement = connection.createStatement();
             var rs = statement.executeQuery("SELECT TOP 1 1")) {
            Assertions.assertTrue(rs.next());
        }
    }

    @Test
    void wrongUrlConnectionTest() {
        var connector = new MsJdbcConnector("jdbc:sqlserver://localhost:5432/broken");
        Assertions.assertThrows(IOException.class, connector::getConnection);
    }

    @Test
    void urlValidationFailTest() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new MsJdbcConnector("test"));
    }

    @Test
    void urlValidationTest() {
        Assertions.assertDoesNotThrow(() -> new MsJdbcConnector("jdbc:sqlserver:"));
    }
}