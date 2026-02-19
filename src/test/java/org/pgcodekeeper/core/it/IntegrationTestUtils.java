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
package org.pgcodekeeper.core.it;

import org.junit.jupiter.api.Assertions;
import org.pgcodekeeper.core.ContextLocation;
import org.pgcodekeeper.core.FILES_POSTFIX;
import org.pgcodekeeper.core.TestUtils;
import org.pgcodekeeper.core.api.PgCodeKeeperApi;
import org.pgcodekeeper.core.database.api.IDatabaseProvider;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.api.schema.ObjectLocation;
import org.pgcodekeeper.core.database.api.schema.ObjectReference;
import org.pgcodekeeper.core.database.base.parser.FullAnalyze;
import org.pgcodekeeper.core.model.difftree.DiffTree;
import org.pgcodekeeper.core.model.difftree.TreeElement;
import org.pgcodekeeper.core.settings.CoreSettings;
import org.pgcodekeeper.core.settings.DiffSettings;
import org.pgcodekeeper.core.utils.InputStreamProvider;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public final class IntegrationTestUtils {

    static {
        // explicit locale for tests with localization
        Locale.setDefault(Locale.ENGLISH);
    }

    public static final String RESOURCE_DUMP = "testing_dump.sql";
    public static final String RESOURCE_MS_DUMP = "testing_ms_dump.sql";
    public static final List<String> IGNORED_SCHEMAS_LIST = List.of("worker", "country", "ignore1", "ignore4vrw");

    public static IDatabase loadTestDump(IDatabaseProvider databaseProvider, String resource, Class<?> c,
                                         DiffSettings diffSettings)
            throws IOException, InterruptedException {
        return loadTestDump(databaseProvider, resource, c, diffSettings, true);
    }

    public static IDatabase loadTestDump(IDatabaseProvider databaseProvider, String resource, Class<?> c,
                                         DiffSettings diffSettings, boolean analysis)
            throws IOException, InterruptedException {
        InputStreamProvider input = () -> c.getResourceAsStream(resource);
        String inputObjectName = "test/" + c.getName() + '/' + resource;

        var loader = databaseProvider.getDumpLoader(input, inputObjectName, diffSettings);

        IDatabase db = loader.load();
        if (analysis) {
            FullAnalyze.fullAnalyze(db, diffSettings.getErrors());
        }

        assertErrors(diffSettings.getErrors());
        return db;
    }

    public static void assertErrors(List<Object> errors) {
        var errorsString = errors.stream()
                .map(Object::toString)
                .collect(Collectors.joining(System.lineSeparator()));

        Assertions.assertEquals("", errorsString);
    }

    public static void assertDiffSame(IDatabaseProvider provider, IDatabase db, String template,
                                      DiffSettings diffSettings)
            throws IOException, InterruptedException {
        assertDiff(provider, db, db, diffSettings, "File name template: " + template);
    }

    public static void assertDiff(IDatabaseProvider provider, IDatabase oldDb, IDatabase newDb,
                                  DiffSettings diffSettings, String errorMessage)
            throws IOException, InterruptedException {
        String script = PgCodeKeeperApi.diff(provider, oldDb, newDb, diffSettings);
        Assertions.assertEquals("", script.trim(), errorMessage);
    }

    public static void assertResult(String script, String template, Class<?> clazz) throws IOException {
        String expected = TestUtils.readResource(template + FILES_POSTFIX.DIFF_SQL, clazz).trim();
        String actual = script.trim();

        TestUtils.assertIgnoreNewLines(expected, actual);
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

    public static void assertDiff(IDatabaseProvider databaseProvider, String fileNameTemplate, Class<?> clazz)
            throws IOException, InterruptedException {
        var diffSettings = new DiffSettings(new CoreSettings());
        String script = getScript(databaseProvider, fileNameTemplate, diffSettings, clazz);
        assertResult(script, fileNameTemplate, clazz);
    }

    public static String getScript(IDatabaseProvider databaseProvider, String fileNameTemplate,
                                   DiffSettings diffSettings, Class<?> clazz)
            throws IOException, InterruptedException {
        var dbOld = loadTestDump(databaseProvider, fileNameTemplate + FILES_POSTFIX.ORIGINAL_SQL, clazz, diffSettings);
        assertDiffSame(databaseProvider, dbOld, fileNameTemplate, diffSettings);

        var dbNew = loadTestDump(databaseProvider, fileNameTemplate + FILES_POSTFIX.NEW_SQL, clazz, diffSettings);
        assertDiffSame(databaseProvider, dbNew, fileNameTemplate, diffSettings);

        return PgCodeKeeperApi.diff(databaseProvider, dbOld, dbNew, diffSettings);
    }

    public static void assertEqualsDependencies(IDatabaseProvider databaseProvider, String dbTemplate,
                                                String userTemplateName, Map<String, DbObjType> selected,
                                                Class<?> clazz, DiffSettings diffSettings)
            throws IOException, InterruptedException {
        var oldDbFull = loadTestDump(databaseProvider, dbTemplate + FILES_POSTFIX.ORIGINAL_SQL, clazz, diffSettings);
        var newDbFull = loadTestDump(databaseProvider, dbTemplate + FILES_POSTFIX.NEW_SQL, clazz, diffSettings);

        assertDiffSame(databaseProvider, oldDbFull, dbTemplate, diffSettings);
        assertDiffSame(databaseProvider, newDbFull, dbTemplate, diffSettings);

        TreeElement tree = DiffTree.create(diffSettings.getSettings(), oldDbFull, newDbFull, null);

        setSelected(selected, tree, oldDbFull, newDbFull);
        String script = PgCodeKeeperApi.diff(databaseProvider, oldDbFull, newDbFull, diffSettings, tree);
        var userSelTemplate = null == userTemplateName ? dbTemplate : dbTemplate + "_" + userTemplateName;
        assertResult(script, userSelTemplate, clazz);
    }

    /**
     * In this method we simulate user behavior where he selected some objects, all
     * or none.
     *
     * @param selectedObjects - {@link Map} selected objects, where key - name of object, value - {@link DbObjType} of
     *                        object. If is null user select all objects
     * @param tree            - {@link TreeElement} after diff old and new database state
     * @param oldDbFull       - old state of {@link IDatabase}
     * @param newDbFull       - new state of {@link IDatabase}
     */
    private static void setSelected(Map<String, DbObjType> selectedObjects, TreeElement tree, IDatabase oldDbFull,
                                    IDatabase newDbFull) {
        if (null == selectedObjects) {
            tree.setAllChecked();
            return;
        }

        for (var sel : selectedObjects.entrySet()) {
            String[] arr = Arrays.copyOf(sel.getKey().split("-"), 3);
            var ref = new ObjectReference(arr[0], arr[1], arr[2], sel.getValue());
            var stmt = newDbFull.getStatement(ref);
            if (null == stmt) {
                stmt = oldDbFull.getStatement(ref);
            }
            tree.findElement(stmt).setSelected(true);
        }
    }

    public static String getRefsAsString(Map<String, Set<ObjectLocation>> refs) {
        StringBuilder sb = new StringBuilder();

        for (Map.Entry<String, Set<ObjectLocation>> entry : refs.entrySet()) {
            entry.getValue().stream().sorted(Comparator.comparingInt(ContextLocation::getOffset)).forEach(loc -> {
                sb.append("Reference: ");
                ObjectReference ref = loc.getObjectReference();
                if (ref != null) {
                    sb.append("Object = ").append(ref).append(", ");
                }
                sb.append("action = ").append(loc.getAction()).append(", ");
                sb.append("offset = ").append(loc.getOffset()).append(", ");
                sb.append("line number = ").append(loc.getLineNumber()).append(", ");
                sb.append("charPositionInLine = ").append(loc.getCharPositionInLine());
                sb.append('\n');
            });
        }

        return sb.toString();
    }

    private IntegrationTestUtils() {
    }
}
