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
package org.pgcodekeeper.core;

import org.junit.jupiter.api.Assertions;
import org.pgcodekeeper.core.loader.DatabaseLoader;
import org.pgcodekeeper.core.loader.PgDumpLoader;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.model.difftree.DiffTree;
import org.pgcodekeeper.core.model.difftree.TreeElement;
import org.pgcodekeeper.core.schema.AbstractDatabase;
import org.pgcodekeeper.core.schema.AbstractSchema;
import org.pgcodekeeper.core.schema.GenericColumn;
import org.pgcodekeeper.core.schema.PgObjLocation;
import org.pgcodekeeper.core.schema.ch.ChSchema;
import org.pgcodekeeper.core.schema.ms.MsSchema;
import org.pgcodekeeper.core.schema.pg.PgSchema;
import org.pgcodekeeper.core.settings.CoreSettings;
import org.pgcodekeeper.core.settings.ISettings;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class TestUtils {

    static {
        // explicit locale for tests with localization
        Locale.setDefault(Locale.ENGLISH);
    }

    public static final String RESOURCE_DUMP = "testing_dump.sql";
    public static final String RESOURCE_MS_DUMP = "testing_ms_dump.sql";
    public static final List<String> IGNORED_SCHEMAS_LIST = List.of("worker", "country", "ignore1", "ignore4vrw");

    public static AbstractDatabase loadTestDump(String resource, Class<?> c, ISettings settings)
            throws IOException, InterruptedException {
        return loadTestDump(resource, c, settings, true);
    }

    public static AbstractDatabase loadTestDump(String resource, Class<?> c, ISettings settings, boolean analysis)
            throws IOException, InterruptedException {
        PgDumpLoader loader = new PgDumpLoader(() -> c.getResourceAsStream(resource),
                "test/" + c.getName() + '/' + resource, settings);
        AbstractDatabase db = analysis ? loader.loadAndAnalyze() : loader.load();
        Assertions.assertEquals("[]", loader.getErrors().toString(), "Test resource caused loader errors!");
        return db;
    }

    public static AbstractDatabase createDumpDB(DatabaseType dbType) {
        var settings = new CoreSettings();
        settings.setDbType(dbType);
        AbstractDatabase db = DatabaseLoader.createDb(settings);
        AbstractSchema schema = switch (dbType) {
            case PG -> new PgSchema(Consts.PUBLIC);
            case MS -> new MsSchema(Consts.DBO);
            case CH -> new ChSchema(Consts.CH_DEFAULT_DB);
        };
        db.addSchema(schema);
        db.setDefaultSchema(schema.getName());
        PgObjLocation loc = new PgObjLocation.Builder()
                .setObject(new GenericColumn(schema.getName(), DbObjType.SCHEMA))
                .build();
        schema.setLocation(loc);
        return db;
    }

    public static void runDiffSame(AbstractDatabase db, String template, ISettings settings)
            throws IOException, InterruptedException {
        String script = new PgDiff(settings).diff(db, db, null);
        Assertions.assertEquals("", script.trim(), "File name template: " + template);
    }

    public static void compareResult(String script, String template,
                                     Class<?> clazz) throws IOException {
        Assertions.assertEquals(
                readResource(template + FILES_POSTFIX.DIFF_SQL, clazz).trim(),
                script.trim());
    }

    public static String readResource(String resourceName, Class<?> clazz) throws IOException {
        try (InputStream is = clazz.getResourceAsStream(resourceName)) {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    public static void createIgnoredSchemaFile(Path dir) throws IOException {
        String rule = """
                SHOW ALL
                HIDE NONE country
                HIDE NONE worker
                HIDE REGEX 'ignore.*'""";
        Files.writeString(dir.resolve(".pgcodekeeperignoreschema"), rule);
    }

    public static void createIgnoreListFile(Path dir) throws IOException {
        String rule = """
                SHOW ALL
                HIDE REGEX 'people.*'""";
        Files.writeString(dir.resolve(".pgcodekeeperignore"), rule);
    }

    public static Path getPathToResource(String resourceName, Class<?> clazz) throws URISyntaxException {
        URL url = clazz.getResource(resourceName);
        return Paths.get(url.toURI());
    }

    static void runDiff(String fileNameTemplate, DatabaseType dbType, Class<?> clazz) throws IOException, InterruptedException {
        var settings = new CoreSettings();
        settings.setDbType(dbType);
        String script = getScript(fileNameTemplate, settings, clazz);
        TestUtils.compareResult(script, fileNameTemplate, clazz);
    }

    static String getScript(String fileNameTemplate, CoreSettings settings, Class<?> clazz)
            throws IOException, InterruptedException {
        return getScript(fileNameTemplate, settings, clazz, false);
    }

    static String getScript(String fileNameTemplate, CoreSettings settings, Class<?> clazz, boolean needTransaction)
            throws IOException, InterruptedException {
        AbstractDatabase dbOld = TestUtils.loadTestDump(fileNameTemplate + FILES_POSTFIX.ORIGINAL_SQL, clazz, settings);
        TestUtils.runDiffSame(dbOld, fileNameTemplate, settings);

        AbstractDatabase dbNew = TestUtils.loadTestDump(fileNameTemplate + FILES_POSTFIX.NEW_SQL, clazz, settings);
        TestUtils.runDiffSame(dbNew, fileNameTemplate, settings);

        settings.setAddTransaction(needTransaction);
        return new PgDiff(settings).diff(dbOld, dbNew, null);
    }

    public static void testDepcy(String dbTemplate, String userTemplateName, Map<String, DbObjType> selectedObjs,
                                 Class<?> clazz, ISettings settings) throws IOException, InterruptedException {
        AbstractDatabase oldDbFull = TestUtils.loadTestDump(dbTemplate + FILES_POSTFIX.ORIGINAL_SQL, clazz, settings);
        AbstractDatabase newDbFull = TestUtils.loadTestDump(dbTemplate + FILES_POSTFIX.NEW_SQL, clazz, settings);

        TestUtils.runDiffSame(oldDbFull, dbTemplate, settings);
        TestUtils.runDiffSame(newDbFull, dbTemplate, settings);

        TreeElement tree = DiffTree.create(settings, oldDbFull, newDbFull, null);

        setSelected(selectedObjs, tree, oldDbFull, newDbFull);
        String script = new PgDiff(settings).diff(tree, oldDbFull, newDbFull, null, null, null);
        var userSelTemplate = null == userTemplateName ? dbTemplate : dbTemplate + "_" + userTemplateName;
        TestUtils.compareResult(script, userSelTemplate, clazz);
    }

    /**
     * In this method we simulate user behavior where he selected some objects, all
     * or none.
     *
     * @param selectedObjects - {@link Map} selected objects, where key - name of object, value - {@link DbObjType} of
     *                        object. If is null user select all objects
     * @param tree            - {@link TreeElement} after diff old and new database state
     * @param oldDbFull       - old state of {@link AbstractDatabase}
     * @param newDbFull       - new state of {@link AbstractDatabase}
     */
    private static void setSelected(Map<String, DbObjType> selectedObjects, TreeElement tree, AbstractDatabase oldDbFull,
                                    AbstractDatabase newDbFull) {
        if (null == selectedObjects) {
            tree.setAllChecked();
            return;
        }

        for (var sel : selectedObjects.entrySet()) {
            String[] arr = Arrays.copyOf(sel.getKey().split("-"), 3);
            var col = new GenericColumn(arr[0], arr[1], arr[2], sel.getValue());
            var stmt = newDbFull.getStatement(col);
            if (null == stmt) {
                stmt = oldDbFull.getStatement(col);
            }
            tree.findElement(stmt).setSelected(true);
        }
    }

    private TestUtils() {
    }
}
