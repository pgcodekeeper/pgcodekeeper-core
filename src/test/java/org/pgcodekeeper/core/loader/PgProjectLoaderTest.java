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
package org.pgcodekeeper.core.loader;

import org.eclipse.core.runtime.SubMonitor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.DatabaseType;
import org.pgcodekeeper.core.TestUtils;
import org.pgcodekeeper.core.ignoreparser.IgnoreParser;
import org.pgcodekeeper.core.model.difftree.*;
import org.pgcodekeeper.core.model.exporter.ModelExporter;
import org.pgcodekeeper.core.schema.AbstractDatabase;
import org.pgcodekeeper.core.schema.AbstractSchema;
import org.pgcodekeeper.core.settings.CoreSettings;
import org.pgcodekeeper.core.utils.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

class PgProjectLoaderTest {

    @Test
    void testProjectLoaderWithIgnoredSchemas() throws IOException, InterruptedException {
        try (TempDir tempDir = new TempDir("ignore-schemas-test-project")) {
            Path dir = tempDir.get();
            var settings = new CoreSettings();

            AbstractDatabase dbDump = TestUtils.loadTestDump(TestUtils.RESOURCE_DUMP, TestUtils.class, settings);

            new ModelExporter(dir, dbDump, DatabaseType.PG, Consts.UTF_8, settings).exportFull();

            TestUtils.createIgnoredSchemaFile(dir);
            Path listFile = dir.resolve(".pgcodekeeperignoreschema");

            // load ignored schema list
            IgnoreSchemaList ignoreSchemaList = new IgnoreSchemaList();
            IgnoreParser ignoreParser = new IgnoreParser(ignoreSchemaList);
            ignoreParser.parse(listFile);

            AbstractDatabase loader = new ProjectLoader(dir.toString(), settings, SubMonitor.convert(null), null,
                    ignoreSchemaList).load();

            for (AbstractSchema dbSchema : loader.getSchemas()) {
                if (TestUtils.IGNORED_SCHEMAS_LIST.contains(dbSchema.getName())) {
                    Assertions.fail("Ignored Schema loaded " + dbSchema.getName());
                } else {
                    Assertions.assertEquals(
                            dbDump.getSchema(dbSchema.getName()), dbSchema, "Schema from dump isn't equal schema from loader");
                }
            }
        }
    }

    @Test
    void testModelExporterWithIgnoredLists() throws IOException, InterruptedException {
        try (TempDir tempDir = new TempDir("new-project")) {
            Path dir = tempDir.get();
            var settings = new CoreSettings();

            AbstractDatabase dbDump = TestUtils.loadTestDump(TestUtils.RESOURCE_DUMP, TestUtils.class, settings);
            TreeElement root = DiffTree.create(dbDump, null, null);
            root.setAllChecked();

            TestUtils.createIgnoreListFile(dir);
            Path listFile = dir.resolve(".pgcodekeeperignore");

            IgnoreList ignoreList = new IgnoreList();
            IgnoreParser ignoreParser = new IgnoreParser(ignoreList);
            ignoreParser.parse(listFile);

            List<TreeElement> selected = new TreeFlattener()
                    .onlySelected()
                    .useIgnoreList(ignoreList)
                    .onlyTypes(settings.getAllowedTypes())
                    .flatten(root);

            new ModelExporter(dir, dbDump, null, DatabaseType.PG, selected, Consts.UTF_8, settings)
                    .exportProject();

            AbstractDatabase loader = new ProjectLoader(dir.toString(), settings).load();
            var objects = loader.getDescendants().toList();
            var ignoredObj = "people";
            for (var st : objects) {
                var stName = st.getName();
                if (stName.startsWith(ignoredObj)) {
                    Assertions.fail("Ignored object loaded " + stName);
                }
            }
        }
    }
}
