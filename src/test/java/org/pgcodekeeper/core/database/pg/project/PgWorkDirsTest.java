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
import org.pgcodekeeper.core.database.base.project.AbstractWorkDirs;
import org.pgcodekeeper.core.database.pg.schema.PgColumn;
import org.pgcodekeeper.core.database.pg.schema.PgMaterializedView;
import org.pgcodekeeper.core.database.pg.schema.PgSchema;
import org.pgcodekeeper.core.database.pg.schema.PgSimpleTable;
import org.pgcodekeeper.core.database.pg.schema.PgView;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

final class PgWorkDirsTest {

    @Test
    void testDefaultMappings() {
        var mapping = new PgWorkDirs().getDirMapping();

        Assertions.assertEquals("SCHEMA", mapping.get("SCHEMA").getDirName());
        Assertions.assertEquals("TABLE", mapping.get("TABLE").getDirName());
        Assertions.assertEquals("TABLE", mapping.get("FOREIGN_TABLE").getDirName());
        Assertions.assertEquals("VIEW", mapping.get("MAT_VIEW").getDirName());
        Assertions.assertEquals("FUNCTION", mapping.get("TRIGGER_FUNC").getDirName());
        Assertions.assertEquals("FDW", mapping.get("FOREIGN_DATA_WRAPPER").getDirName());
        Assertions.assertTrue(new PgWorkDirs().isSplitBySchema());
    }

    @Test
    void testLoadsAltDirsFromPropertiesFile(@TempDir Path tempDir) throws Exception {
        Path altDirsFile = tempDir.resolve(AbstractWorkDirs.ALT_DIRS_FILENAME);
        Files.writeString(altDirsFile,
                "TABLE=ALT_TABLE\nMAT_VIEW=MATERIALIZED_VIEW\nis_split_by_schema=false");

        var workDirs = new PgWorkDirs(altDirsFile);

        Assertions.assertEquals("ALT_TABLE", workDirs.getDirMapping().get("TABLE").getDirName());
        Assertions.assertEquals("MATERIALIZED_VIEW", workDirs.getDirMapping().get("MAT_VIEW").getDirName());
        Assertions.assertEquals("VIEW", workDirs.getDirMapping().get("VIEW").getDirName());
        Assertions.assertFalse(workDirs.isSplitBySchema());
    }

    @Test
    void testLoadsAltDirsFromArbitraryFilename(@TempDir Path tempDir) throws Exception {
        Path altDirsFile = tempDir.resolve("custom-name.properties");
        Files.writeString(altDirsFile, "TABLE=ALT_TABLE\nMAT_VIEW=MATERIALIZED_VIEW");

        var workDirs = new PgWorkDirs(altDirsFile);

        Assertions.assertEquals("ALT_TABLE", workDirs.getDirMapping().get("TABLE").getDirName());
        Assertions.assertEquals("MATERIALIZED_VIEW", workDirs.getDirMapping().get("MAT_VIEW").getDirName());
    }

    @Test
    void testGetRelativeFilePathWalksUpSubElement() {
        var schema = new PgSchema("public");
        var table = new PgSimpleTable("my_table");
        schema.addChild(table);
        var column = new PgColumn("col1");
        table.addColumn(column);

        Path result = new PgWorkDirs().getRelativeFilePath(column);

        Assertions.assertEquals(Path.of("SCHEMA", "public", "TABLE", "my_table.sql"), result);
    }

    @Test
    void testSpecificRuleWinsOverGeneric(@TempDir Path tempDir) throws Exception {
        Path altDirsFile = tempDir.resolve(AbstractWorkDirs.ALT_DIRS_FILENAME);
        Files.writeString(altDirsFile, "MAT_VIEW=MATERIALIZED_VIEW");
        var workDirs = new PgWorkDirs(altDirsFile);

        var schema = new PgSchema("public");
        var regularView = new PgView("my_view");
        var matView = new PgMaterializedView("my_mat_view");
        schema.addChild(regularView);
        schema.addChild(matView);

        Assertions.assertEquals(
                Path.of("SCHEMA", "public", "VIEW", "my_view.sql"),
                workDirs.getRelativeFilePath(regularView));
        Assertions.assertEquals(
                Path.of("SCHEMA", "public", "MATERIALIZED_VIEW", "my_mat_view.sql"),
                workDirs.getRelativeFilePath(matView));
    }

    @Test
    void testSaveAltDirs(@TempDir Path tempDir) throws Exception {
        Path altDirsFile = tempDir.resolve(AbstractWorkDirs.ALT_DIRS_FILENAME);
        Files.writeString(altDirsFile, "TRIGGER_FUNC=TRIGGER_FUNCTION\nTABLE=ALT_TABLE");

        var workDirs = new PgWorkDirs(altDirsFile);

        Path outputDir = tempDir.resolve("output");
        Files.createDirectories(outputDir);
        workDirs.saveAltDirs(outputDir);

        var props = new Properties();
        try (var reader = Files.newBufferedReader(outputDir.resolve(AbstractWorkDirs.ALT_DIRS_FILENAME))) {
            props.load(reader);
        }

        Assertions.assertEquals("TRIGGER_FUNCTION", props.getProperty("TRIGGER_FUNC"));
        Assertions.assertEquals("ALT_TABLE", props.getProperty("TABLE"));
        Assertions.assertEquals("VIEW", props.getProperty("VIEW"));
        Assertions.assertEquals("VIEW", props.getProperty("MAT_VIEW"));
        Assertions.assertEquals("SCHEMA", props.getProperty("SCHEMA"));
        Assertions.assertEquals("true", props.getProperty(AbstractWorkDirs.IS_SPLIT_BY_SCHEMA));
        Assertions.assertEquals(workDirs.getDirMapping().size() + 1, props.size());
    }

    @Test
    void testSaveDefaultAltDirs(@TempDir Path tempDir) throws Exception {
        var workDirs = new PgWorkDirs();
        workDirs.saveAltDirs(tempDir);
        boolean exists = Files.isRegularFile(tempDir.resolve(AbstractWorkDirs.ALT_DIRS_FILENAME));
        Assertions.assertFalse(exists);
    }

    @Test
    void testTopDirNames() {
        var workDirs = new PgWorkDirs();
        var expected = List.of("SCHEMA", "EXTENSION", "EVENT_TRIGGER", "USER_MAPPING", "CAST", "SERVER", "FDW");
        var actual = workDirs.getTopLevelDirNames();
        Assertions.assertEquals(expected, actual);
    }
}
