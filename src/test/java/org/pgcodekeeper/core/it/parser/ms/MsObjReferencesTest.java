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
package org.pgcodekeeper.core.it.parser.ms;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pgcodekeeper.core.FILES_POSTFIX;
import org.pgcodekeeper.core.TestUtils;
import org.pgcodekeeper.core.database.api.schema.DatabaseType;
import org.pgcodekeeper.core.database.base.schema.AbstractDatabase;
import org.pgcodekeeper.core.database.ms.loader.MsDumpLoader;
import org.pgcodekeeper.core.it.IntegrationTestUtils;
import org.pgcodekeeper.core.database.base.parser.ParserListenerMode;
import org.pgcodekeeper.core.settings.CoreSettings;

import java.io.IOException;

class MsObjReferencesTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "ms_aggregate",
            "ms_assemblies",
            "ms_authorizations",
            "ms_availability_group",
            "ms_backup",
            "ms_broker_priority",
            "ms_certificates",
            "ms_control_flow",
            "ms_cursors",
            "ms_database",
            "ms_delete",
            "ms_drop",
            "ms_endpoint",
            "ms_event",
            "ms_full_width_chars",
            "ms_function",
            "ms_index",
            "ms_insert",
            "ms_key",
            "ms_logins",
            "ms_merge",
            "ms_other",
            "ms_predicates",
            "ms_procedures",
            "ms_restore",
            "ms_roles",
            "ms_rule",
            "ms_schema",
            "ms_select",
            "ms_select_match",
            "ms_sequences",
            "ms_server",
            "ms_statements",
            "ms_table",
            "ms_transactions",
            "ms_triggers",
            "ms_type",
            "ms_update",
            "ms_users",
            "ms_view",
            "ms_xml_data_type",
            "ms_statistics",
    })
    void compareMsReferences(final String fileNameTemplate) throws IOException, InterruptedException {
        var settings = new CoreSettings();
        settings.setDbType(DatabaseType.MS);

        String resource = fileNameTemplate + FILES_POSTFIX.SQL;
        var loader = new MsDumpLoader(() -> getClass().getResourceAsStream(resource), resource, settings);
        loader.setMode(ParserListenerMode.REF);
        AbstractDatabase db = loader.load();

        String expected = TestUtils
                .readResource(fileNameTemplate + FILES_POSTFIX.REFS_TXT, getClass()).strip();

        String actual = IntegrationTestUtils.getRefsAsString(db.getObjReferences()).strip();

        Assertions.assertEquals("[]", loader.getErrors().toString());
        Assertions.assertEquals(expected, actual);
    }
}
