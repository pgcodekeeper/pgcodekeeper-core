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
package org.pgcodekeeper.core.it.parser.ch;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pgcodekeeper.core.DatabaseType;
import org.pgcodekeeper.core.FILES_POSTFIX;
import org.pgcodekeeper.core.TestUtils;
import org.pgcodekeeper.core.it.IntegrationTestUtils;
import org.pgcodekeeper.core.loader.ParserListenerMode;
import org.pgcodekeeper.core.loader.PgDumpLoader;
import org.pgcodekeeper.core.schema.AbstractDatabase;
import org.pgcodekeeper.core.settings.CoreSettings;

import java.io.IOException;

class ChObjReferencesTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "ch_database",
            "ch_function",
            "ch_index",
            "ch_insert",
            "ch_other",
            "ch_show",
            "ch_view",
            "ch_policy",
            "ch_table",
            "ch_select",
            "ch_privileges",
            "ch_user",
            "ch_role",
            "ch_dictionary",
            "ch_update",
            "ch_delete"
    })
    void compareChReferences(final String fileNameTemplate) throws IOException, InterruptedException {
        var settings = new CoreSettings();
        settings.setDbType(DatabaseType.CH);

        String resource = fileNameTemplate + FILES_POSTFIX.SQL;
        PgDumpLoader loader = new PgDumpLoader(() -> getClass().getResourceAsStream(resource), resource, settings);
        loader.setMode(ParserListenerMode.REF);
        AbstractDatabase db = loader.load();

        String expected = TestUtils
                .readResource(fileNameTemplate + FILES_POSTFIX.REFS_TXT, getClass()).strip();

        String actual = IntegrationTestUtils.getRefsAsString(db.getObjReferences()).strip();

        Assertions.assertEquals("[]", loader.getErrors().toString());
        Assertions.assertEquals(expected, actual);
    }
}
