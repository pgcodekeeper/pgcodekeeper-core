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
import org.pgcodekeeper.core.database.ms.schema.MsColumn;
import org.pgcodekeeper.core.database.ms.schema.MsSchema;
import org.pgcodekeeper.core.database.ms.schema.MsTable;

import java.nio.file.Path;

final class MsWorkDirsTest {

    @Test
    void testGetRelativeFilePathForTable() {
        var schema = new MsSchema("dbo");
        var table = new MsTable("my_table");
        schema.addTable(table);

        Path result = MsWorkDirs.getRelativeFilePath(table);

        Path expectedTablePath = Path.of("Tables", "dbo.my_table.sql");
        Assertions.assertEquals(expectedTablePath, result);
    }

    @Test
    void testGetRelativeFilePathForColumn() {
        var schema = new MsSchema("dbo");
        var table = new MsTable("my_table");
        schema.addTable(table);
        var column = new MsColumn("col1");
        table.addColumn(column);

        Path result = MsWorkDirs.getRelativeFilePath(column);

        Path expectedTablePath = Path.of("Tables", "dbo.my_table.sql");
        Assertions.assertEquals(expectedTablePath, result);
    }
}