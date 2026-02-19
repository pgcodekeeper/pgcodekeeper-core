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
package org.pgcodekeeper.core.it.loader.ms;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.database.ms.MsDatabaseProvider;
import org.pgcodekeeper.core.database.ms.project.MsModelExporter;
import org.pgcodekeeper.core.it.IntegrationTestUtils;
import org.pgcodekeeper.core.settings.CoreSettings;
import org.pgcodekeeper.core.settings.DiffSettings;

import java.io.IOException;
import java.nio.file.Path;

import static org.pgcodekeeper.core.it.IntegrationTestUtils.*;

/**
 * Tests for MS SQL ProjectLoader functionality
 */
class MsProjectLoaderTest {

    @Test
    void testProjectLoaderWithIgnoredSchemas(@TempDir Path dir) throws IOException, InterruptedException {
        var settings = new CoreSettings();
        MsDatabaseProvider databaseProvider = new MsDatabaseProvider();
        DiffSettings diffSettings = new DiffSettings(settings);
        var msDbDump = loadTestDump(databaseProvider, RESOURCE_MS_DUMP, IntegrationTestUtils.class, diffSettings);

        new MsModelExporter(dir, msDbDump, Consts.UTF_8, settings).exportFull();

        createIgnoredSchemaFile(dir);

        var db = databaseProvider.getProjectLoader(dir, diffSettings).load();

        for (var dbSchema : db.getSchemas()) {
            if (IGNORED_SCHEMAS_LIST.contains(dbSchema.getName())) {
                Assertions.fail("Ignored Schema loaded " + dbSchema.getName());
            } else {
                Assertions.assertEquals(msDbDump.getSchema(dbSchema.getName()), dbSchema,
                        "Schema from ms dump isn't equal schema from loader");
            }
        }
    }
}
