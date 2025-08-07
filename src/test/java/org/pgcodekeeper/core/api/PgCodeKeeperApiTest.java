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
package org.pgcodekeeper.core.api;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pgcodekeeper.core.PgCodekeeperException;
import org.pgcodekeeper.core.schema.AbstractDatabase;
import org.pgcodekeeper.core.settings.CoreSettings;
import org.pgcodekeeper.core.utils.NullMonitor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class PgCodeKeeperApiTest {

    private static final String ORIGINAL = "_original.sql";
    private static final String NEW = "_new.sql";
    private static final String DIFF = "_diff.sql";

    private static final String PUBLIC_DIRECTORY = "SCHEMA/public/";
    private static final String TABLES_DIRECTORY = PUBLIC_DIRECTORY + "TABLE/";

    private CoreSettings settings;

    @BeforeEach
    protected void initSettings() {
        settings = new CoreSettings();
    }

    @Test
    void testDiff() throws PgCodekeeperException, IOException, InterruptedException {
        var testName = "test_diff";
        var oldDb = loadDatabaseFromDump(testName + ORIGINAL);
        var newDb = loadDatabaseFromDump(testName + NEW);
        var expectedDiff = getExpectedDiff(testName);

        String actual = PgCodeKeeperApi.diff(settings, oldDb, newDb);

        assertEquals(expectedDiff, actual);
    }

    @Test
    void testDiffWithIgnoreList() throws PgCodekeeperException, IOException, InterruptedException {
        var testName = "test_ignore";
        var oldDb = loadDatabaseFromDump(testName + ORIGINAL);
        var newDb = loadDatabaseFromDump(testName + NEW);
        var ignoreListPath = getFilePath("ignore.pgcodekeeperignore");
        var expectedDiff = getExpectedDiff(testName);

        String actual = PgCodeKeeperApi.diff(settings, oldDb, newDb, List.of(ignoreListPath));

        assertEquals(expectedDiff, actual);
    }

    @Test
    void testExport(@TempDir Path tempDir) throws PgCodekeeperException, IOException, InterruptedException {
        var dumpFileName = "test_export.sql";
        var db = loadDatabaseFromDump(dumpFileName);
        var exportedTableFile = tempDir.resolve(TABLES_DIRECTORY + "test_table.sql");
        var expectedContent = getFileContent(dumpFileName);

        PgCodeKeeperApi.export(settings, db, tempDir.toString());

        assertFileContent(exportedTableFile, expectedContent);
    }

    @Test
    void testExportWithIgnoreList(@TempDir Path tempDir) throws PgCodekeeperException, IOException, InterruptedException {
        var db = loadDatabaseFromDump("test_export_with_ignore_list.sql");
        var exportedTableFile = tempDir.resolve(TABLES_DIRECTORY + "test_table.sql");
        var ignoredTableFile = tempDir.resolve(TABLES_DIRECTORY + "ignored_table.sql");
        var expectedContent = getFileContent("test_export_with_ignore_list_exported.sql");
        var ignoreListPath = getFilePath("test_export_with_ignore_list.pgcodekeeperignore");

        PgCodeKeeperApi.export(settings, db, tempDir.toString(), List.of(ignoreListPath), new NullMonitor());

        assertFileContent(exportedTableFile, expectedContent);
        assertFalse(Files.exists(ignoredTableFile));
    }

    @Test
    void testUpdateProject(@TempDir Path tempDir) throws PgCodekeeperException, IOException, InterruptedException {
        // Setup project structure with initial tables
        setupUpdateProjectStructure(tempDir);
        var oldDb = DatabaseFactory.loadFromProject(settings, tempDir.toString());
        var newDb = loadDatabaseFromDump("test_update_project_new_dump.sql");
        var expectedContent = getFileContent("test_update_project_new_dump.sql");

        Path tablesDir = tempDir.resolve(TABLES_DIRECTORY);
        Path firstTableFile = tablesDir.resolve("first_table.sql");
        Path secondTableFile = tablesDir.resolve("second_table.sql");

        PgCodeKeeperApi.update(settings, oldDb, newDb, tempDir.toString());

        // Verify first table was removed and second table was updated
        assertFalse(Files.exists(firstTableFile));
        assertFileContent(secondTableFile, expectedContent);
    }

    @Test
    void testUpdateProjectWithIgnoreList(@TempDir Path tempDir) throws PgCodekeeperException, IOException, InterruptedException {
        // Setup project structure with initial tables
        setupUpdateProjectStructure(tempDir);
        var oldDb = DatabaseFactory.loadFromProject(settings, tempDir.toString(), null, null);
        var newDb = loadDatabaseFromDump("test_update_project_new_dump.sql");
        var ignoreListPath = getFilePath("test_update_project_ignore_list.pgcodekeeperignore");

        var expectedFirstTableContent = getFileContent("test_update_project_old_first_table.sql");
        var expectedSecondTableContent = getFileContent("test_update_project_new_dump.sql");

        Path tablesDir = tempDir.resolve(TABLES_DIRECTORY);
        Path firstTableFile = tablesDir.resolve("first_table.sql");
        Path secondTableFile = tablesDir.resolve("second_table.sql");

        PgCodeKeeperApi.update(settings, oldDb, newDb, tempDir.toString(), List.of(ignoreListPath), new NullMonitor());

        // Verify both tables exist and have correct content
        assertFileContent(firstTableFile, expectedFirstTableContent);
        assertFileContent(secondTableFile, expectedSecondTableContent);
    }

    private void setupUpdateProjectStructure(Path tempDir) throws IOException {
        Path publicDir = tempDir.resolve(PUBLIC_DIRECTORY);
        Path tablesDir = tempDir.resolve(TABLES_DIRECTORY);
        Files.createDirectories(tablesDir);

        Path schemaFile = publicDir.resolve("public.sql");
        Path firstTableFile = tablesDir.resolve("first_table.sql");
        Path secondTableFile = tablesDir.resolve("second_table.sql");

        // Copy test files to project structure
        Files.copy(Paths.get(getFilePath("test_update_project_old_public.sql")), schemaFile, StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get(getFilePath("test_update_project_old_first_table.sql")), firstTableFile, StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get(getFilePath("test_update_project_old_second_table.sql")), secondTableFile, StandardCopyOption.REPLACE_EXISTING);
    }

    private static String getFilePath(String fileName) {
        var resource = PgCodeKeeperApiTest.class.getResource(fileName);
        if (resource == null) {
            throw new IllegalArgumentException("Test resource not found: " + fileName);
        }
        return resource.toString().substring(5);
    }

    private String getFileContent(String fileName) throws IOException {
        var path = Path.of(getFilePath(fileName));
        return Files.readString(path);
    }

    private AbstractDatabase loadDatabaseFromDump(String fileName) throws PgCodekeeperException, IOException, InterruptedException {
        return DatabaseFactory.loadFromDump(settings, getFilePath(fileName), new NullMonitor());
    }

    private String getExpectedDiff(String baseName) throws IOException {
        return getFileContent(baseName + DIFF);
    }

    private void assertFileContent(Path filePath, String expectedContent) throws IOException {
        assertTrue(Files.exists(filePath));
        var actualContent = Files.readString(filePath);
        assertEquals(expectedContent, actualContent);
    }


}
