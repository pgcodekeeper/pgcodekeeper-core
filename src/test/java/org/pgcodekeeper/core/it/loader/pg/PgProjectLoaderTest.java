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
package org.pgcodekeeper.core.it.loader.pg;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.TestUtils;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.database.api.schema.ObjectReference;
import org.pgcodekeeper.core.database.base.loader.AbstractProjectLoader;
import org.pgcodekeeper.core.database.pg.PgDatabaseProvider;
import org.pgcodekeeper.core.database.pg.project.PgModelExporter;
import org.pgcodekeeper.core.ignorelist.IgnoreList;
import org.pgcodekeeper.core.ignorelist.IgnoreParser;
import org.pgcodekeeper.core.it.IntegrationTestUtils;
import org.pgcodekeeper.core.library.Library;
import org.pgcodekeeper.core.library.LibraryXmlStore;
import org.pgcodekeeper.core.model.difftree.DiffTree;
import org.pgcodekeeper.core.model.difftree.TreeElement;
import org.pgcodekeeper.core.model.difftree.TreeFlattener;
import org.pgcodekeeper.core.settings.CoreSettings;
import org.pgcodekeeper.core.settings.DiffSettings;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.pgcodekeeper.core.it.IntegrationTestUtils.*;

/**
 * Tests for PostgreSQL ProjectLoader functionality
 */
class PgProjectLoaderTest {

    private static final String RESOURCE_FIRST_LIB = "lib_first_table.sql";
    private static final String RESOURCE_SECOND_LIB = "lib_second_table.sql";
    private static final String RESOURCE_OVERRIDE_EMP = "override_emp.sql";

    private final PgDatabaseProvider databaseProvider = new PgDatabaseProvider();

    @Test
    void testProjectLoaderWithIgnoredSchemas(@TempDir Path dir) throws IOException, InterruptedException {
        var settings = new CoreSettings();
        DiffSettings diffSettings = new DiffSettings(settings);
        IDatabase dbDump = loadTestDump(databaseProvider, RESOURCE_DUMP, IntegrationTestUtils.class, diffSettings);

        new PgModelExporter(dir, dbDump, Consts.UTF_8, settings).exportFull();

        createIgnoredSchemaFile(dir);

        IDatabase db = databaseProvider.getProjectLoader(dir, diffSettings).load();

        for (var dbSchema : db.getSchemas()) {
            if (IGNORED_SCHEMAS_LIST.contains(dbSchema.getName())) {
                Assertions.fail("Ignored Schema loaded " + dbSchema.getName());
            } else {
                Assertions.assertEquals(dbDump.getSchema(dbSchema.getName()), dbSchema,
                        "Schema from dump isn't equal schema from loader");
            }
        }
    }

    @Test
    void testModelExporterWithIgnoredLists(@TempDir Path dir) throws IOException, InterruptedException {
        var settings = new CoreSettings();
        var diffSettings = new DiffSettings(settings);

        IDatabase dbDump = loadTestDump(databaseProvider, RESOURCE_DUMP, IntegrationTestUtils.class, diffSettings);
        TreeElement root = DiffTree.create(settings, dbDump, null, null);
        root.setAllChecked();

        createIgnoreListFile(dir);
        Path listFile = dir.resolve(AbstractProjectLoader.IGNORE_FILE);

        IgnoreList ignoreList = new IgnoreList();
        IgnoreParser ignoreParser = new IgnoreParser(ignoreList);
        ignoreParser.parse(listFile);

        List<TreeElement> selected = new TreeFlattener().onlySelected().useIgnoreList(ignoreList)
                .onlyTypes(settings.getAllowedTypes()).flatten(root);

        new PgModelExporter(dir, dbDump, null, selected, Consts.UTF_8, settings).exportProject();

        IDatabase loader = databaseProvider.getProjectLoader(dir, diffSettings).load();
        var ignoredObj = "people";
        boolean result = loader.getDescendants().map(IStatement::getName).noneMatch(ignoredObj::startsWith);
        assertTrue(result);
    }

    @Test
    void testProjectLoaderWithoutAutoLoad(@TempDir Path dir) throws IOException, InterruptedException {
        var settings = new CoreSettings();
        settings.setDisableAutoLoad(true);
        var diffSettings = new DiffSettings(settings);
        Path projectDir = dir.resolve("project");

        createProject(projectDir, diffSettings);

        String libPath = TestUtils.getFilePath(RESOURCE_FIRST_LIB, getClass()).toString();
        new LibraryXmlStore(projectDir.resolve(LibraryXmlStore.FILE_NAME)).writeDependencies(List.of(
                new Library("", libPath, false, "")), false);

        createIgnoredSchemaFile(projectDir);
        createIgnoreListFile(projectDir);

        IDatabase db = databaseProvider.getProjectLoader(projectDir, diffSettings).load();

        boolean result = db.getSchemas().stream().map(IStatement::getName).anyMatch(IGNORED_SCHEMAS_LIST::contains);
        assertTrue(result, "Ignored schemas not loaded");
        assertTrue(diffSettings.getIgnoreList().getList().isEmpty());

        assertNotLoaded(db, "lib_first_table");
    }

    @Test
    void testProjectLoaderWithLibrary(@TempDir Path dir) throws IOException, InterruptedException {
        var settings = new CoreSettings();
        var diffSettings = new DiffSettings(settings);
        Path projectDir = dir.resolve("project");
        createProject(projectDir, diffSettings);

        String libPath = TestUtils.getFilePath(RESOURCE_FIRST_LIB, getClass()).toString();

        IDatabase db = databaseProvider.getProjectLoader(projectDir, diffSettings, Collections.emptyList(),
                List.of(libPath), Collections.emptyList(), dir.resolve("meta")).load();

        assertLibLoaded(db, "lib_first_table", true);
    }

    @Test
    void testProjectLoaderWithLibraryWithoutPrivileges(@TempDir Path dir) throws IOException, InterruptedException {
        var settings = new CoreSettings();
        var diffSettings = new DiffSettings(settings);
        Path projectDir = dir.resolve("project");
        createProject(projectDir, diffSettings);

        String libPath = TestUtils.getFilePath(RESOURCE_FIRST_LIB, getClass()).toString();

        IDatabase db = databaseProvider.getProjectLoader(projectDir, diffSettings, Collections.emptyList(),
                Collections.emptyList(), List.of(libPath), dir.resolve("meta")).load();

        assertLibLoaded(db, "lib_first_table", false);
    }

    @Test
    void testProjectLoaderWithLibraryFromXml(@TempDir Path dir) throws IOException, InterruptedException {
        var settings = new CoreSettings();
        var diffSettings = new DiffSettings(settings);
        Path projectDir = dir.resolve("project");
        createProject(projectDir, diffSettings);

        String libWithPrivPath = TestUtils.getFilePath(RESOURCE_FIRST_LIB, getClass()).toString();
        String libWithoutPrivPath = TestUtils.getFilePath(RESOURCE_SECOND_LIB, getClass()).toString();

        Path depsFile = projectDir.resolve(LibraryXmlStore.FILE_NAME);
        new LibraryXmlStore(depsFile).writeDependencies(List.of(
                new Library("", libWithPrivPath, false, ""),
                new Library("", libWithoutPrivPath, true, "")), false);

        IDatabase db = databaseProvider.getProjectLoader(projectDir, diffSettings, List.of(depsFile.toString()),
                Collections.emptyList(), Collections.emptyList(), dir.resolve("meta")).load();

        assertLibLoaded(db, "lib_first_table", true);
        assertLibLoaded(db, "lib_second_table", false);
    }

    @Test
    void testProjectLoaderWithIsLibTrue(@TempDir Path dir) throws IOException, InterruptedException {
        var settings = new CoreSettings();
        var diffSettings = new DiffSettings(settings);
        Path projectDir = dir.resolve("project");
        createProject(projectDir, diffSettings);

        String libPath = TestUtils.getFilePath(RESOURCE_FIRST_LIB, getClass()).toString();
        new LibraryXmlStore(projectDir.resolve(LibraryXmlStore.FILE_NAME)).writeDependencies(List.of(
                new Library("", libPath, false, "")), false);

        String externalLibPath = TestUtils.getFilePath(RESOURCE_SECOND_LIB, getClass()).toString();
        Path externalXml = dir.resolve(".external");
        new LibraryXmlStore(externalXml).writeDependencies(List.of(
                new Library("", externalLibPath, false, "")), false);

        var loader = databaseProvider.getProjectLoader(projectDir, diffSettings, List.of(externalXml.toString()),
                Collections.emptyList(), Collections.emptyList(), dir.resolve("meta"));
        loader.setLib(true);
        IDatabase db = loader.load();

        assertNotLoaded(db, "lib_first_table");
        assertNotLoaded(db, "lib_second_table");
    }

    @Test
    void testProjectLoaderWithOverrides(@TempDir Path dir) throws IOException, InterruptedException {
        var settings = new CoreSettings();
        var diffSettings = new DiffSettings(settings);
        Path projectDir = dir.resolve("project");
        createProject(projectDir, diffSettings);

        Path overrideDir = projectDir.resolve("OVERRIDES/SCHEMA/public/TABLE");
        Files.createDirectories(overrideDir);
        var empPath = TestUtils.getFilePath(RESOURCE_OVERRIDE_EMP, getClass());
        Files.copy(empPath, overrideDir.resolve("emp.sql"));

        IDatabase db = databaseProvider.getProjectLoader(projectDir, diffSettings).load();

        var emp = db.getStatement(new ObjectReference("public", "emp", DbObjType.TABLE));
        Assertions.assertNotNull(emp);
        var hasSelectGrant = emp.getPrivileges().stream().anyMatch(
                p -> !p.isRevoke() && "SELECT".equals(p.getPermission())
                        && "override_user".equals(p.getRole()));

        Assertions.assertEquals("override_user", emp.getOwner());
        Assertions.assertTrue(hasSelectGrant);
    }

    private void assertLibLoaded(IDatabase db, String tableName, boolean isWithPrivileges) {
        var libTableRef = new ObjectReference("public", tableName, DbObjType.TABLE);
        var libTable = db.getStatement(libTableRef);

        Assertions.assertNotNull(libTable);
        Assertions.assertTrue(libTable.isLib());

        if (isWithPrivileges) {
            Assertions.assertEquals("lib_user", libTable.getOwner());
            Assertions.assertFalse(libTable.getPrivileges().isEmpty());
        } else {
            Assertions.assertNull(libTable.getOwner());
            Assertions.assertTrue(libTable.getPrivileges().isEmpty());
        }
    }

    private void assertNotLoaded(IDatabase db, String tableName) {
        var libTableRef = new ObjectReference("public", tableName, DbObjType.TABLE);
        var libTable = db.getStatement(libTableRef);

        Assertions.assertNull(libTable);
    }

    private void createProject(Path dir, DiffSettings diffSettings)
            throws IOException, InterruptedException {
        IDatabase dbDump = loadTestDump(databaseProvider, RESOURCE_DUMP, IntegrationTestUtils.class, diffSettings);
        new PgModelExporter(dir, dbDump, Consts.UTF_8, diffSettings.getSettings()).exportFull();
    }

}
