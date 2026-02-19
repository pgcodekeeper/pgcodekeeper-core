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
package org.pgcodekeeper.core.it.diff.pg;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pgcodekeeper.core.FILES_POSTFIX;
import org.pgcodekeeper.core.api.PgCodeKeeperApi;
import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.pg.PgDatabaseProvider;
import org.pgcodekeeper.core.it.IntegrationTestUtils;
import org.pgcodekeeper.core.settings.CoreSettings;
import org.pgcodekeeper.core.settings.DiffSettings;

import java.io.IOException;

import static org.pgcodekeeper.core.it.IntegrationTestUtils.loadTestDump;

/**
 * Tests for PostgreSQL database migrate data option .
 *
 * @author Gulnaz Khazieva
 */
class PgMoveDataDiffTest {

    /**
     * Template name for file names that should be used for the test. Testing
     * method adds _original.sql, _new.sql and _diff.sql to the file name
     * template.
     */
    @ParameterizedTest
    @ValueSource(strings = {
            "move_data",
            //implementation for data movement test in PG for case with different columns (with identity columns)
            "move_data_diff_cols_identity",
            //implementation for foreign table data movement test in PG
            "move_data_foreign",
            //implementation for data movement test in PG (with identity columns)
            "move_data_identity",
            //implementation for partition table data movement test in PG
            "move_data_partition_table",
            //implementation for partition table data movement test in PG (with identity columns)
            "move_data_partition_table_identity",
            //implementation for partition table data movement test in PG with identity columns(dropped, changed identity col)
            "move_data_change_col_identity"
    })
    void moveDataTest(String fileNameTemplate) throws IOException, InterruptedException {
        var settings = new CoreSettings();
        PgDatabaseProvider databaseProvider = new PgDatabaseProvider();
        settings.setDataMovementMode(true);
        var diffSettings = new DiffSettings(settings);
        IDatabase dbOld = loadTestDump(
                databaseProvider, fileNameTemplate + FILES_POSTFIX.ORIGINAL_SQL, PgMoveDataDiffTest.class, diffSettings);
        IDatabase dbNew = loadTestDump(
                databaseProvider, fileNameTemplate + FILES_POSTFIX.NEW_SQL, PgMoveDataDiffTest.class, diffSettings);

        IntegrationTestUtils.assertDiffSame(databaseProvider, dbOld, fileNameTemplate, diffSettings);
        IntegrationTestUtils.assertDiffSame(databaseProvider, dbNew, fileNameTemplate, diffSettings);

        String script = PgCodeKeeperApi.diff(databaseProvider, dbOld, dbNew, diffSettings);
        String content = script.replaceAll("([0-9a-fA-F]{32})", "randomly_generated_part");

        IntegrationTestUtils.assertResult(content, fileNameTemplate, PgMoveDataDiffTest.class);
    }
}
