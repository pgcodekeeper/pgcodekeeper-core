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
package org.pgcodekeeper.core.it.jdbc.ch;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.pgcodekeeper.core.utils.testcontainer.TestContainerType;
import org.pgcodekeeper.core.FILES_POSTFIX;
import org.pgcodekeeper.core.database.api.schema.DatabaseType;
import org.pgcodekeeper.core.database.ch.ChDatabaseProvider;
import org.pgcodekeeper.core.database.ch.jdbc.ChJdbcConnector;
import org.pgcodekeeper.core.it.jdbc.base.JdbcLoaderTest;
import org.pgcodekeeper.core.settings.CoreSettings;

class ChJdbcLoaderTest extends JdbcLoaderTest {

    @ParameterizedTest
    @CsvSource({
            "ch_database",
            "ch_dump_test",
            "ch_mat_view",
            "ch_table",
            "ch_view"
    })
    void chJdbcLoaderTest(String fileName) throws Exception {
        var settings = new CoreSettings();
        settings.setDbType(DatabaseType.CH);
        var url = TestContainerType.CH_24.getUrl();
        jdbcLoaderTest(fileName + FILES_POSTFIX.SQL, "ch.pgcodekeeperignore",
                url, new ChJdbcConnector(url), settings, null, getClass(), new ChDatabaseProvider());
    }
}
