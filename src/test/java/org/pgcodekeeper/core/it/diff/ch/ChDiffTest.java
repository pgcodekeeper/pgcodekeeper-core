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
package org.pgcodekeeper.core.it.diff.ch;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pgcodekeeper.core.DatabaseType;
import org.pgcodekeeper.core.settings.CoreSettings;

import java.io.IOException;

import static org.pgcodekeeper.core.it.IntegrationTestUtils.*;

class ChDiffTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "add_ch_columns_at_table",
            "add_ch_database",
            "add_ch_dictionary",
            "add_ch_function",
            "add_ch_policy",
            "add_ch_privilegies",
            "add_ch_role",
            "add_ch_tables_engine_merge_tree",
            "add_ch_user",
            "add_ch_view",
            "alter_ch_columns_at_table",
            "alter_ch_table",
            "alter_ch_table_engine",
            "alter_ch_view",
            "change_ch_database",
            "change_ch_dictionary",
            "change_ch_policy",
            "compare_ch_database_without_diffs",
            "compare_ch_engine_no_diffs",
            "compare_ch_function",
            "compare_ch_privilegies",
            "compare_ch_role",
            "compare_ch_user",
            "drop_ch_column_at_table",
            "drop_ch_database",
            "drop_ch_dictionary",
            "drop_ch_function",
            "drop_ch_policy",
            "drop_ch_role",
            "drop_ch_table",
            "drop_ch_user",
            "drop_ch_view",
            "revoke_ch_privilegies"

    })
    void diffTest(String fileNameTemplate) throws IOException, InterruptedException {
        assertDiff(fileNameTemplate, DatabaseType.CH, ChDiffTest.class);
    }

    /**
     * test case when ignore privileges option enable
     */
    @ParameterizedTest
    @ValueSource(strings = {
            "ch_ignore_privillegies"
    })
    void ignorePrivilegesTest(String fileNameTemplate) throws IOException, InterruptedException {
        var settings = new CoreSettings();
        settings.setIgnorePrivileges(true);
        settings.setDbType(DatabaseType.CH);
        String script = getScript(fileNameTemplate, settings, ChDiffTest.class);
        assertResult(script, fileNameTemplate, ChDiffTest.class);
    }
}
