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
package org.pgcodekeeper.core.api;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.IOException;
import java.nio.file.*;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.pgcodekeeper.core.TestUtils;
import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.base.project.AbstractWorkDirs;
import org.pgcodekeeper.core.database.pg.PgDatabaseProvider;
import org.pgcodekeeper.core.database.pg.project.PgWorkDirs;
import org.pgcodekeeper.core.settings.CoreSettings;
import org.pgcodekeeper.core.settings.ISettings;

class PgCodeKeeperApiTest {

    private static final String ORIGINAL = "_original.sql";
    private static final String NEW = "_new.sql";
    private static final String DIFF = "_diff.sql";

    private static final String PUBLIC_DIRECTORY = "SCHEMA/public/";
    private static final String TABLES_DIRECTORY = PUBLIC_DIRECTORY + "TABLE/";

    private ISettings settings;
    private final PgDatabaseProvider provider = new PgDatabaseProvider();

    @BeforeEach
    void initSettings() {
        settings = new CoreSettings();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "test_diff"
    })
    void diffTest(String testName) throws IOException, InterruptedException {
        var oldDbLoader = provider.getDumpLoader(getFilePath(testName + ORIGINAL), settings);
        var newDbLoader = provider.getDumpLoader(getFilePath(testName + NEW), settings);
        var expectedDiff = getExpectedDiff(testName);

        String actual = PgCodeKeeperApi.diff(provider, oldDbLoader, newDbLoader, settings);

        TestUtils.assertIgnoreNewLines(expectedDiff, actual);
        TestUtils.assertErrors(settings.getErrors());
    }


    @ParameterizedTest
    @CsvSource({
            "test_diff, false",
            "test_diff, true"})
    void loaderTest(String testName, boolean parallelLoad) throws IOException, InterruptedException {
        CoreSettings settings = new CoreSettings();
        settings.setParallelLoad(parallelLoad);

        var oldDbLoader = provider.getDumpLoader(getFilePath(testName + ORIGINAL), settings);
        var newDbLoader = provider.getDumpLoader(getFilePath(testName + NEW), settings);
        var expectedDiff = getExpectedDiff(testName);

        String actual = PgCodeKeeperApi.diff(provider, oldDbLoader, newDbLoader, settings);

        TestUtils.assertIgnoreNewLines(expectedDiff, actual);
        TestUtils.assertErrors(settings.getErrors());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "test_ignore"
    })
    void diffWithIgnoreListTest(String testName) throws IOException, InterruptedException {
        var oldDbLoader = provider.getDumpLoader(getFilePath(testName + ORIGINAL), settings);
        var newDbLoader = provider.getDumpLoader(getFilePath(testName + NEW), settings);
        var ignoreListPath = getFilePath("ignore.pgcodekeeperignore");
        var expectedDiff = getExpectedDiff(testName);

        settings.addIgnoreList(ignoreListPath);
        String actual = PgCodeKeeperApi.diff(provider, oldDbLoader, newDbLoader, settings);

        TestUtils.assertIgnoreNewLines(expectedDiff, actual);
        TestUtils.assertErrors(settings.getErrors());
    }

    @Test
    void exportTest(@TempDir Path tempDir) throws IOException, InterruptedException {
        var dumpFileName = "test_export.sql";
        var loader = provider.getDumpLoader(getFilePath(dumpFileName), settings);
        var exportedTableFile = tempDir.resolve(TABLES_DIRECTORY + "test_table.sql");
        var expectedContent = getFileContent(dumpFileName);

        PgCodeKeeperApi.exportToProject(provider, null, loader, tempDir, settings);

        assertFileContent(exportedTableFile, expectedContent);
        TestUtils.assertErrors(settings.getErrors());
    }

    @Test
    void exportWithIgnoreListTest(@TempDir Path tempDir) throws IOException, InterruptedException {
        var loader = provider.getDumpLoader(getFilePath("test_export_with_ignore_list.sql"), settings);
        var exportedTableFile = tempDir.resolve(TABLES_DIRECTORY + "test_table.sql");
        var ignoredTableFile = tempDir.resolve(TABLES_DIRECTORY + "ignored_table.sql");
        var expectedContent = getFileContent("test_export_with_ignore_list_exported.sql");
        var ignoreListPath = getFilePath("test_export_with_ignore_list.pgcodekeeperignore");

        settings.addIgnoreList(ignoreListPath);
        PgCodeKeeperApi.exportToProject(provider, null, loader, tempDir, settings);

        assertFileContent(exportedTableFile, expectedContent);
        assertFalse(Files.exists(ignoredTableFile));
        TestUtils.assertErrors(settings.getErrors());
    }

    @Test
    void updateProjectTest(@TempDir Path tempDir) throws IOException, InterruptedException {
        // Setup project structure with initial tables
        setupUpdateProjectStructure(tempDir);
        var oldDbLoader = provider.getProjectLoader(tempDir, settings);
        var newDbLoader = provider.getDumpLoader(getFilePath("test_update_project_new_dump.sql"), settings);
        var expectedContent = getFileContent("test_update_project_new_dump.sql");

        Path tablesDir = tempDir.resolve(TABLES_DIRECTORY);
        Path firstTableFile = tablesDir.resolve("first_table.sql");
        Path secondTableFile = tablesDir.resolve("second_table.sql");

        PgCodeKeeperApi.exportToProject(provider, oldDbLoader, newDbLoader, tempDir, settings);

        // Verify first table was removed and second table was updated
        assertFalse(Files.exists(firstTableFile));
        assertFileContent(secondTableFile, expectedContent);
        TestUtils.assertErrors(settings.getErrors());
    }

    @Test
    void updateProjectWithIgnoreListTest(@TempDir Path tempDir) throws IOException, InterruptedException {
        // Setup project structure with initial tables
        setupUpdateProjectStructure(tempDir);
        var oldDbLoader = provider.getProjectLoader(tempDir, settings);
        var newDbLoader = provider.getDumpLoader(getFilePath("test_update_project_new_dump.sql"), settings);
        var ignoreListPath = getFilePath("test_update_project_ignore_list.pgcodekeeperignore");

        var expectedFirstTableContent = getFileContent("test_update_project_old_first_table.sql");
        var expectedSecondTableContent = getFileContent("test_update_project_new_dump.sql");

        Path tablesDir = tempDir.resolve(TABLES_DIRECTORY);
        Path firstTableFile = tablesDir.resolve("first_table.sql");
        Path secondTableFile = tablesDir.resolve("second_table.sql");

        settings.addIgnoreList(ignoreListPath);
        PgCodeKeeperApi.exportToProject(provider, oldDbLoader, newDbLoader, tempDir, settings);

        // Verify both tables exist and have correct content
        assertFileContent(firstTableFile, expectedFirstTableContent);
        assertFileContent(secondTableFile, expectedSecondTableContent);
        TestUtils.assertErrors(settings.getErrors());
    }

    @Test
    void fullUpdatePreservesMetadataDirsTest(@TempDir Path tempDir) throws IOException, InterruptedException {
        Path settingsDir = tempDir.resolve(".settings");
        Files.createDirectories(settingsDir);

        IDatabase newDb = provider.getDumpLoader(getFilePath("test_export.sql"), settings).loadAndAnalyze();
        provider.getProjectUpdater(newDb, null, List.of(), tempDir, false, new CoreSettings()).updateFull(true);

        assertTrue(Files.exists(settingsDir));
        TestUtils.assertErrors(settings.getErrors());
    }

    @Test
    void fullUpdateCleansRenamedDirsAndKeepsForeignTest(@TempDir Path tempDir)
            throws IOException, InterruptedException {
        Files.createDirectories(tempDir.resolve("MIGRATION"));
        Files.createDirectories(tempDir.resolve("SCHEMA"));
        Files.writeString(tempDir.resolve(AbstractWorkDirs.ALT_DIRS_FILENAME), "SCHEMA=DB\n");

        IDatabase newDb = provider.getDumpLoader(getFilePath("test_export.sql"), settings).loadAndAnalyze();
        provider.getProjectUpdater(newDb, null, List.of(), tempDir, false, new CoreSettings())
                .updateFull(true, new PgWorkDirs());

        assertTrue(Files.exists(tempDir.resolve("MIGRATION")));
        assertFalse(Files.exists(tempDir.resolve("SCHEMA")));
        TestUtils.assertErrors(settings.getErrors());
    }

    private void setupUpdateProjectStructure(Path tempDir) throws IOException {
        Path publicDir = tempDir.resolve(PUBLIC_DIRECTORY);
        Path tablesDir = tempDir.resolve(TABLES_DIRECTORY);
        Files.createDirectories(tablesDir);

        Path schemaFile = publicDir.resolve("public.sql");
        Path firstTableFile = tablesDir.resolve("first_table.sql");
        Path secondTableFile = tablesDir.resolve("second_table.sql");

        // Copy test files to project structure
        Files.copy(getFilePath("test_update_project_old_public.sql"), schemaFile, StandardCopyOption.REPLACE_EXISTING);
        Files.copy(getFilePath("test_update_project_old_first_table.sql"), firstTableFile, StandardCopyOption.REPLACE_EXISTING);
        Files.copy(getFilePath("test_update_project_old_second_table.sql"), secondTableFile, StandardCopyOption.REPLACE_EXISTING);
    }

    private Path getFilePath(String fileName) {
        return TestUtils.getFilePath(fileName, getClass());
    }

    private String getFileContent(String fileName) throws IOException {
        return Files.readString(getFilePath(fileName));
    }

    private String getExpectedDiff(String baseName) throws IOException {
        return getFileContent(baseName + DIFF);
    }

    private void assertFileContent(Path filePath, String expectedContent) throws IOException {
        assertTrue(Files.exists(filePath));
        var actualContent = Files.readString(filePath);
        TestUtils.assertIgnoreNewLines(expectedContent, actualContent);
    }
}
