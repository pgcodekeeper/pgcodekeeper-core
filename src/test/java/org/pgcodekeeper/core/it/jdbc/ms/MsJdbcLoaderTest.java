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
package org.pgcodekeeper.core.it.jdbc.ms;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.pgcodekeeper.core.FILES_POSTFIX;
import org.pgcodekeeper.core.TestContainer;
import org.pgcodekeeper.core.database.api.schema.DatabaseType;
import org.pgcodekeeper.core.database.ms.MsDatabaseProvider;
import org.pgcodekeeper.core.database.ms.jdbc.MsJdbcConnector;
import org.pgcodekeeper.core.it.jdbc.base.JdbcLoaderTest;
import org.pgcodekeeper.core.settings.CoreSettings;

class MsJdbcLoaderTest extends JdbcLoaderTest {

    @ParameterizedTest
    @CsvSource({
            "ms_dump_test",
            "ms_index",
            "ms_sequence",
            "ms_statistics",
            "ms_sys_ver_table",
            "ms_table_tracking",
            "ms_table_type",
            "ms_table_with_partition",
            "ms_trigger",
            "ms_view",
    })
    void msJdbcLoaderTest(String fileName) throws Exception {
        var settings = new CoreSettings();
        settings.setDbType(DatabaseType.MS);
        jdbcLoaderTest(fileName + FILES_POSTFIX.SQL, "ms.pgcodekeeperignore",
                TestContainer.MS_URL, new MsJdbcConnector(TestContainer.MS_URL), settings, null, getClass(), new MsDatabaseProvider());
    }

    @ParameterizedTest
    @CsvSource({
            "ms_table_type",
    })
    void msJdbcLoaderWithMemomyOptimizedTest(String fileName) throws Exception {
        var settings = new CoreSettings();
        settings.setDbType(DatabaseType.MS);
        jdbcLoaderTest(fileName + "_memory_optimized" + FILES_POSTFIX.SQL, "ms.pgcodekeeperignore",
                TestContainer.MS_URL_MEMORY_OPTIMIZED, new MsJdbcConnector(TestContainer.MS_URL_MEMORY_OPTIMIZED),
                settings, null, getClass(), true, new MsDatabaseProvider());
    }
}
