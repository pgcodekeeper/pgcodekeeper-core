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
package org.pgcodekeeper.core.it.jdbc.pg;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.pgcodekeeper.core.utils.testcontainer.TestContainerType;
import org.pgcodekeeper.core.FILES_POSTFIX;
import org.pgcodekeeper.core.database.pg.PgDatabaseProvider;
import org.pgcodekeeper.core.database.pg.jdbc.PgJdbcConnector;
import org.pgcodekeeper.core.it.jdbc.base.JdbcLoaderTest;
import org.pgcodekeeper.core.settings.CoreSettings;

import java.util.Locale;

class PgGpJdbcLoaderTest extends JdbcLoaderTest {

    @ParameterizedTest
    @CsvSource({
            "dump_test, PG_16",
            "operator, PG_16",
            "statistics, PG_16",
            "view, PG_16",
            "dump_test, GP_6",
            "operator, GP_6",
            "view, GP_6",
            "dump_test, GP_7",
            "operator, GP_7",
            "statistics, GP_7",
            "view, GP_7",
    })
    void pgGpJdbcLoaderTest(String fileName, String contTypeName) throws Exception {
        var contType = TestContainerType.valueOf(contTypeName);
        var lowerCaseTypeName = contTypeName.toLowerCase(Locale.ROOT);
        var url = contType.getUrl();
        jdbcLoaderTest(lowerCaseTypeName + "_" + fileName + FILES_POSTFIX.SQL,
                lowerCaseTypeName + ".pgcodekeeperignore", url, new PgJdbcConnector(url),
                new CoreSettings(), contType.getVersion(), getClass(), new PgDatabaseProvider());
    }
}
