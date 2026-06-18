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
package org.pgcodekeeper.core.database.ms.project;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pgcodekeeper.core.database.base.project.AbstractWorkDirs;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

final class MsWorkDirsTest {

    @Test
    void testDefaultMappings() {
        var mapping = new MsWorkDirs().getDirMapping();

        Assertions.assertEquals("Security/Schemas", mapping.get("SCHEMA").getDirName());
        Assertions.assertEquals("Security/Roles", mapping.get("ROLE").getDirName());
        Assertions.assertEquals("Security/Users", mapping.get("USER").getDirName());
        Assertions.assertEquals("Tables", mapping.get("TABLE").getDirName());
        Assertions.assertEquals("Views", mapping.get("VIEW").getDirName());
        Assertions.assertEquals("Stored Procedures", mapping.get("PROCEDURE").getDirName());
        Assertions.assertFalse(new MsWorkDirs().isSplitBySchema());
    }

    @Test
    void testLoadsAltDirsFromPropertiesFile(@TempDir Path tempDir) throws Exception {
        Path altDirsFile = tempDir.resolve(AbstractWorkDirs.ALT_DIRS_FILENAME);
        Files.writeString(altDirsFile,
                "TABLE=ALT_Tables\nPROCEDURE=Procs\nis_split_by_schema=true");

        var workDirs = new MsWorkDirs(altDirsFile);

        Assertions.assertEquals("ALT_Tables", workDirs.getDirMapping().get("TABLE").getDirName());
        Assertions.assertEquals("Procs", workDirs.getDirMapping().get("PROCEDURE").getDirName());
        Assertions.assertEquals("Views", workDirs.getDirMapping().get("VIEW").getDirName());
        Assertions.assertTrue(workDirs.isSplitBySchema());
    }

    @Test
    void testTopDirNames() {
        var workDirs = new MsWorkDirs();
        var expected = List.of("Security", "Assemblies", "Types", "Tables", "Views", "Sequences", "Functions", "Stored Procedures");
        var actual = workDirs.getTopLevelDirNames();
        Assertions.assertEquals(expected, actual);
    }
}
