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
package org.pgcodekeeper.core.database.pg.project;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.FILES_POSTFIX;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.database.api.schema.ObjectReference;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.database.pg.PgDatabaseProvider;
import org.pgcodekeeper.core.database.pg.project.PgModelExporter;
import org.pgcodekeeper.core.it.IntegrationTestUtils;
import org.pgcodekeeper.core.script.SQLScript;
import org.pgcodekeeper.core.settings.CoreSettings;

import java.io.IOException;
import java.util.Arrays;

/**
 * Test for PostgreSQL database model export functionality
 */
class PgModelExporterTest {

    @ParameterizedTest
    @CsvSource({
            "test_settings_in_pg, public.t1, TABLE",
            "test_settings_in_pg, public.t1.constr, CONSTRAINT"
    })
    void settingsTest(String template, String stmtName, DbObjType type) throws IOException, InterruptedException {
        var settings = new CoreSettings();
        PgDatabaseProvider databaseProvider = new PgDatabaseProvider();
        settings.setGenerateConstraintNotValid(true);
        settings.setGenerateExists(true);

        IDatabase db = IntegrationTestUtils.loadTestDump(databaseProvider, template + FILES_POSTFIX.SQL, getClass(), settings);

        var exporter = new PgModelExporter(null, db, Consts.UTF_8, settings);
        var stmt = getStatement(db, stmtName, type);
        var actual = exporter.getDumpSql(stmt);

        // check that exporter generate script is equals generated script with default settings
        Assertions.assertEquals(getCreationSQL(stmt), actual, "this should be equals");

        var script = new SQLScript(settings, db.getSeparator());
        stmt.getCreationSQL(script);

        // check that exporter generates script is not equals generated script with user settings
        Assertions.assertNotEquals(script.getFullScript(), actual, "this should be not equals");
    }

    /**
     * get {@link AbstractStatement} object from {@link IDatabase}
     *
     * @param db       - {@link IDatabase} witch store PgStatement
     * @param stmtName - name of {@link AbstractStatement} witch we need for test
     * @param type     - {@link DbObjType} of {@link AbstractStatement} witch we need for test
     * @return - {@link AbstractStatement} witch we need for test
     */
    private IStatement getStatement(IDatabase db, String stmtName, DbObjType type) {
        String[] arr = Arrays.copyOf(stmtName.split("\\."), 3);
        return db.getStatement(new ObjectReference(arr[0], arr[1], arr[2], type));
    }

    /**
     * generate SQL script with default settings
     *
     * @param statement - {@link AbstractStatement} witch we used for test
     * @return - generated script
     */
    private String getCreationSQL(IStatement statement) {
        var script = new SQLScript(new CoreSettings(), statement.getSeparator());
        statement.getCreationSQL(script);
        return script.getFullScript();
    }
}
