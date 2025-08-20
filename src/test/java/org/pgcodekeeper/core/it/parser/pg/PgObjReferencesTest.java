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
package org.pgcodekeeper.core.it.parser.pg;

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


class PgObjReferencesTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "aggregates",
            "alter_table",
            "arrays",
            "case",
            "cluster",
            "collate",
            "conversion",
            "copy",
            "create_cast",
            "create_function",
            "create_misc",
            "create_procedure",
            "create_table",
            "create_table_like",
            "database",
            "date",
            "delete",
            "dependency",
            "domain",
            "drop_if_exists",
            "drop_operator",
            "enum",
            "event_trigger",
            "extension",
            "fast_default",
            "float8",
            "foreign_data",
            "foreign_key",
            "fts_configuration",
            "fts_parser",
            "fts_template",
            "functional_deps",
            "geometry",
            "groupingsets",
            "index",
            "inherit",
            "insert",
            "insert_conflict",
            "interval",
            "join",
            "json_encoding",
            "jsonb",
            "lseg",
            "merge",
            "misc_functions",
            "misc_sanity",
            "name",
            "namespace",
            "numeric",
            "numeric_big",
            "numerology",
            "oid",
            "oidjoins",
            "operator",
            "opr_sanity",
            "other",
            "partition_aggregate",
            "partition_join",
            "partition_prune",
            "plancache",
            "point",
            "policy",
            "polygon",
            "polymorphism",
            "privileges",
            "publication",
            "rangefuncs",
            "rangetypes",
            "reloptions",
            "role",
            "rowtypes",
            "rules",
            "schema",
            "select",
            "sequence",
            "server",
            "set",
            "spgist",
            "strings",
            "subscription",
            "subselect",
            "sysviews",
            "time",
            "timestamp",
            "timestamptz",
            "timetz",
            "transactions",
            "triggers",
            "tsdicts",
            "tsearch",
            "type",
            "update",
            "user_mapping",
            "view",
            "window",
            "with",
            "pg_unicode_escaping"
    })
    void comparePgReferences(final String fileNameTemplate) throws IOException, InterruptedException {
        var settings = new CoreSettings();
        settings.setDbType(DatabaseType.PG);

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
