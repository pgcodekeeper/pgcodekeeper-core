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
import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.loader.AbstractProjectLoader;
import org.pgcodekeeper.core.database.ch.loader.ChDumpLoader;
import org.pgcodekeeper.core.database.ch.loader.ChProjectLoader;
import org.pgcodekeeper.core.database.ch.schema.ChDatabase;
import org.pgcodekeeper.core.database.ms.loader.MsDumpLoader;
import org.pgcodekeeper.core.database.ms.loader.MsProjectLoader;
import org.pgcodekeeper.core.database.ms.schema.MsDatabase;
import org.pgcodekeeper.core.database.pg.loader.PgDumpLoader;
import org.pgcodekeeper.core.database.pg.loader.PgProjectLoader;
import org.pgcodekeeper.core.database.pg.schema.PgDatabase;
import org.pgcodekeeper.core.ignorelist.IgnoreSchemaList;
import org.pgcodekeeper.core.database.base.parser.FullAnalyze;
import org.pgcodekeeper.core.model.difftree.DiffTree;
import org.pgcodekeeper.core.model.difftree.TreeElement;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.settings.CoreSettings;
import org.pgcodekeeper.core.settings.ISettings;
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

    public static IDatabase loadTestDump(String resource, Class<?> c, ISettings settings)
            throws IOException, InterruptedException {
        return loadTestDump(resource, c, settings, true);
    }

    public static IDatabase loadTestDump(String resource, Class<?> c, ISettings settings, boolean analysis)
            throws IOException, InterruptedException {
        InputStreamProvider input = () -> c.getResourceAsStream(resource);
        String inputObjectName = "test/" + c.getName() + '/' + resource;
        var loader = switch (settings.getDbType()) {
            case PG -> new PgDumpLoader(input, inputObjectName, settings);
            case MS -> new MsDumpLoader(input, inputObjectName, settings);
            case CH -> new ChDumpLoader(input, inputObjectName, settings);
        };
        IDatabase db = loader.load();
        if (analysis) {
            FullAnalyze.fullAnalyze(db, loader.getErrors());
        }

        var errors = loader.getErrors().stream()
        .map(Object::toString)
        .collect(Collectors.joining(System.lineSeparator()));

        Assertions.assertEquals("", errors, "Test resource caused loader errors!");
        return db;
    }

    public static void assertDiffSame(IDatabase db, String template, ISettings settings)
            throws IOException, InterruptedException {
        assertDiff(db, db, settings, "File name template: " + template);
    }

    public static void assertDiff(IDatabase oldDb, IDatabase newDb, ISettings settings, String errorMessage)
            throws IOException, InterruptedException {
        String script = PgCodeKeeperApi.diff(settings, oldDb, newDb);
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

    public static void assertDiff(String fileNameTemplate, DatabaseType dbType, Class<?> clazz)
            throws IOException, InterruptedException {
        var settings = new CoreSettings();
        settings.setDbType(dbType);
        String script = getScript(fileNameTemplate, settings, clazz);
        assertResult(script, fileNameTemplate, clazz);
    }

    public static String getScript(String fileNameTemplate, CoreSettings settings, Class<?> clazz)
            throws IOException, InterruptedException {
        IDatabase dbOld = loadTestDump(fileNameTemplate + FILES_POSTFIX.ORIGINAL_SQL, clazz, settings);
        assertDiffSame(dbOld, fileNameTemplate, settings);

        IDatabase dbNew = loadTestDump(fileNameTemplate + FILES_POSTFIX.NEW_SQL, clazz, settings);
        assertDiffSame(dbNew, fileNameTemplate, settings);

        return PgCodeKeeperApi.diff(settings, dbOld, dbNew);
    }

    public static void assertEqualsDependencies(String dbTemplate, String userTemplateName,
                                                Map<String, DbObjType> selected, Class<?> clazz, ISettings settings)
            throws IOException, InterruptedException {
        IDatabase oldDbFull = loadTestDump(dbTemplate + FILES_POSTFIX.ORIGINAL_SQL, clazz, settings);
        IDatabase newDbFull = loadTestDump(dbTemplate + FILES_POSTFIX.NEW_SQL, clazz, settings);

        assertDiffSame(oldDbFull, dbTemplate, settings);
        assertDiffSame(newDbFull, dbTemplate, settings);

        TreeElement tree = DiffTree.create(settings, oldDbFull, newDbFull, null);

        setSelected(selected, tree, oldDbFull, newDbFull);
        String script = PgCodeKeeperApi.diff(settings, tree, oldDbFull, newDbFull, null, null, null);
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
            var col = new GenericColumn(arr[0], arr[1], arr[2], sel.getValue());
            var stmt = newDbFull.getStatement(col);
            if (null == stmt) {
                stmt = oldDbFull.getStatement(col);
            }
            tree.findElement(stmt).setSelected(true);
        }
    }

    public static String getRefsAsString(Map<String, Set<ObjectLocation>> refs) {
        StringBuilder sb = new StringBuilder();

        for (Map.Entry<String, Set<ObjectLocation>> entry : refs.entrySet()) {
            entry.getValue().stream().sorted(Comparator.comparingInt(ContextLocation::getOffset)).forEach(loc -> {
                sb.append("Reference: ");
                GenericColumn col = loc.getObj();
                if (col != null) {
                    sb.append("Object = ").append(col).append(", ");
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

    public static AbstractProjectLoader<?> createProjectLoader(Path dirPath, ISettings settings, IDatabase database) {
        return createProjectLoader(dirPath, settings, database, null, null);
    }

    public static AbstractProjectLoader<?> createProjectLoader(Path dirPath, ISettings settings, IDatabase database,
                                                               IMonitor monitor, IgnoreSchemaList ignoreSchemaList) {
        AbstractProjectLoader<?> loader;
        if (database instanceof PgDatabase) {
            loader = new PgProjectLoader(dirPath, settings, monitor, ignoreSchemaList);
        } else if (database instanceof MsDatabase) {
            loader = new MsProjectLoader(dirPath, settings, monitor, ignoreSchemaList);
        } else if (database instanceof ChDatabase) {
            loader = new ChProjectLoader(dirPath, settings, monitor, ignoreSchemaList);
        } else {
            throw new IllegalStateException("Unknown database type");
        }

        return loader;
    }

    private IntegrationTestUtils() {
    }
}
