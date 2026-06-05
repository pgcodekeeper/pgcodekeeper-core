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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.FILES_POSTFIX;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.database.api.schema.ObjectReference;
import org.pgcodekeeper.core.database.base.project.AbstractWorkDirs;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.database.pg.PgDatabaseProvider;
import org.pgcodekeeper.core.database.pg.schema.PgDatabase;
import org.pgcodekeeper.core.database.pg.schema.PgFunction;
import org.pgcodekeeper.core.database.pg.schema.PgSchema;
import org.pgcodekeeper.core.database.pg.schema.PgSimpleTable;
import org.pgcodekeeper.core.it.IntegrationTestUtils;
import org.pgcodekeeper.core.script.SQLScript;
import org.pgcodekeeper.core.settings.CoreSettings;
import org.pgcodekeeper.core.settings.DiffSettings;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * Test for PostgreSQL database model export functionality
 */
class PgModelExporterTest {

    @Test
    void testExporterReadsAltDirsFromOutDir(@TempDir Path tempDir) throws IOException {
        Files.writeString(tempDir.resolve(AbstractWorkDirs.ALT_DIRS_FILENAME),
                "TRIGGER_FUNC=TRIGGER_FUNCTION\nTABLE=ALT_TABLE");

        var exporter = new PgModelExporter(tempDir, null, null, null, Consts.UTF_8, new CoreSettings());

        var schema = new PgSchema("public");
        var table = new PgSimpleTable("my_table");
        var triggerFunc = new PgFunction("my_trigger_func");
        triggerFunc.setReturns("trigger");
        schema.addChild(table);
        schema.addChild(triggerFunc);

        Assertions.assertEquals(Path.of("SCHEMA", "public", "ALT_TABLE", "my_table.sql"),
                exporter.getRelativeFilePath(table));
        Assertions.assertEquals(
                Path.of("SCHEMA", "public", "TRIGGER_FUNCTION", "my_trigger_func.sql"),
                exporter.getRelativeFilePath(triggerFunc));
    }

    @Test
    void testExportFullExcludesLibraryObjects(@TempDir Path tempDir) throws IOException {
        var db = new PgDatabase();
        db.addChild(new PgSchema("proj_schema"));

        var libSchema = new PgSchema("lib_schema");
        libSchema.setLibName("mylib");
        db.addChild(libSchema);

        new PgModelExporter(tempDir, db, Consts.UTF_8, new CoreSettings()).exportFull();

        Assertions.assertTrue(Files.exists(tempDir.resolve(Path.of("SCHEMA", "proj_schema", "proj_schema.sql"))));
        Assertions.assertFalse(Files.exists(tempDir.resolve(Path.of("SCHEMA", "lib_schema"))));
    }

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
        var diffSettings = new DiffSettings(settings);

        IDatabase db = IntegrationTestUtils.loadTestDump(databaseProvider, template + FILES_POSTFIX.SQL,
                getClass(), diffSettings);

        var exporter = new PgModelExporter(null, db, Consts.UTF_8, settings);
        var stmt = getStatement(db, stmtName, type);
        var actual = exporter.getDumpSql(stmt);

        Assertions.assertEquals(getCreationSQL(stmt), actual);

        var script = new SQLScript(diffSettings, db.getSeparator());
        stmt.getCreationSQL(script);

        Assertions.assertNotEquals(script.getFullScript(), actual);
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
        var script = new SQLScript(new DiffSettings(new CoreSettings()), statement.getSeparator());
        statement.getCreationSQL(script);
        return script.getFullScript();
    }
}
