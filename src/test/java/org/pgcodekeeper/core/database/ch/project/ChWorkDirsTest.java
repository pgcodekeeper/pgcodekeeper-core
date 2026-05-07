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
package org.pgcodekeeper.core.database.ch.project;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pgcodekeeper.core.database.base.project.AbstractWorkDirs;

import java.nio.file.Files;
import java.nio.file.Path;

final class ChWorkDirsTest {

    @Test
    void testDefaultMappings() {
        var mapping = new ChWorkDirs().getDirMapping();

        Assertions.assertEquals("DATABASE", mapping.get("SCHEMA").getDirName());
        Assertions.assertEquals("TABLE", mapping.get("TABLE").getDirName());
        Assertions.assertEquals("VIEW", mapping.get("VIEW").getDirName());
        Assertions.assertEquals("DICTIONARY", mapping.get("DICTIONARY").getDirName());
        Assertions.assertEquals("FUNCTION", mapping.get("FUNCTION").getDirName());
        Assertions.assertEquals("ROLE", mapping.get("ROLE").getDirName());
        Assertions.assertTrue(new ChWorkDirs().isSplitBySchema());
    }

    @Test
    void testLoadsAltDirsFromPropertiesFile(@TempDir Path tempDir) throws Exception {
        Path altDirsFile = tempDir.resolve(AbstractWorkDirs.ALT_DIRS_FILENAME);
        Files.writeString(altDirsFile, "TABLE=ALT_TABLE\nDICTIONARY=DICTS\nis_split_by_schema=false");

        var workDirs = new ChWorkDirs(altDirsFile);

        Assertions.assertEquals("ALT_TABLE", workDirs.getDirMapping().get("TABLE").getDirName());
        Assertions.assertEquals("DICTS", workDirs.getDirMapping().get("DICTIONARY").getDirName());
        Assertions.assertEquals("VIEW", workDirs.getDirMapping().get("VIEW").getDirName());
        Assertions.assertFalse(workDirs.isSplitBySchema());
    }
}
