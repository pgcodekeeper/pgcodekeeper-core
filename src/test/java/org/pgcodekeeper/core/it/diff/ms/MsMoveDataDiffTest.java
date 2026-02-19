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
package org.pgcodekeeper.core.it.diff.ms;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pgcodekeeper.core.FILES_POSTFIX;
import org.pgcodekeeper.core.api.PgCodeKeeperApi;
import org.pgcodekeeper.core.database.ms.MsDatabaseProvider;
import org.pgcodekeeper.core.settings.CoreSettings;
import org.pgcodekeeper.core.settings.DiffSettings;

import java.io.IOException;

import static org.pgcodekeeper.core.it.IntegrationTestUtils.*;

/**
 * Tests for MS SQL database migrate data option.
 *
 * @author Gulnaz Khazieva
 */
class MsMoveDataDiffTest {

    /**
     * Template name for file names that should be used for the test. Testing
     * method adds _original.sql, _new.sql and _diff.sql to the file name
     * template.
     */
    @ParameterizedTest
    @ValueSource(strings = {
            // implementation for data movement test in MS (without identity columns)
            "drop_ms_table",
            "move_data_ms",
            "move_data_ms_identity"
    })
    void moveDataTest(String fileNameTemplate) throws IOException, InterruptedException {
        var settings = new CoreSettings();
        MsDatabaseProvider databaseProvider = new MsDatabaseProvider();
        settings.setDataMovementMode(true);
        var diffSettings = new DiffSettings(settings);

        var dbOld = loadTestDump(
                databaseProvider, fileNameTemplate + FILES_POSTFIX.ORIGINAL_SQL, MsMoveDataDiffTest.class, diffSettings);
        var dbNew = loadTestDump(
                databaseProvider, fileNameTemplate + FILES_POSTFIX.NEW_SQL, MsMoveDataDiffTest.class, diffSettings);

        assertDiffSame(databaseProvider, dbOld, fileNameTemplate, diffSettings);
        assertDiffSame(databaseProvider, dbNew, fileNameTemplate, diffSettings);

        String script = PgCodeKeeperApi.diff(databaseProvider, dbOld, dbNew, new DiffSettings(settings));
        String content = script.replaceAll("([0-9a-fA-F]{32})", "randomly_generated_part");

        assertResult(content, fileNameTemplate, MsMoveDataDiffTest.class);
    }
}
