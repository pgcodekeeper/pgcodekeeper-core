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
package org.pgcodekeeper.core.database.pg.project;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.pgcodekeeper.core.database.pg.schema.PgColumn;
import org.pgcodekeeper.core.database.pg.schema.PgSchema;
import org.pgcodekeeper.core.database.pg.schema.PgSimpleTable;

import java.nio.file.Path;

final class PgWorkDirsTest {

    @Test
    void testGetRelativeFilePathForTable() {
        var schema = new PgSchema("public");
        var table = new PgSimpleTable("my_table");
        schema.addChild(table);

        Path result = PgWorkDirs.getRelativeFilePath(table);

        Path expectedTablePath = Path.of("SCHEMA", "public", "TABLE", "my_table.sql");
        Assertions.assertEquals(expectedTablePath, result);
    }

    @Test
    void testGetRelativeFilePathForColumn() {
        var schema = new PgSchema("public");
        var table = new PgSimpleTable("my_table");
        schema.addChild(table);
        var column = new PgColumn("col1");
        table.addColumn(column);

        Path result = PgWorkDirs.getRelativeFilePath(column);

        Path expectedTablePath = Path.of("SCHEMA", "public", "TABLE", "my_table.sql");
        Assertions.assertEquals(expectedTablePath, result);
    }
}
