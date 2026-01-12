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
package org.pgcodekeeper.core.database.ch.formatter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.pgcodekeeper.core.FILES_POSTFIX;
import org.pgcodekeeper.core.TestUtils;
import org.pgcodekeeper.core.database.api.formatter.IndentType;
import org.pgcodekeeper.core.database.base.formatter.FormatConfiguration;

import java.io.IOException;

class ChFormatterTest {

    @Test
    void testCh() throws IOException {
        FormatConfiguration config = new FormatConfiguration();
        config.setIndentSize(2);
        config.setAddWhitespaceAfterOp(true);
        config.setAddWhitespaceBeforeOp(true);
        config.setIndentType(IndentType.WHITESPACE);
        config.setRemoveTrailingWhitespace(true);

        String newFile = TestUtils.readResource("new_ch" + FILES_POSTFIX.SQL, getClass());
        String oldFile = TestUtils.readResource("old_ch" + FILES_POSTFIX.SQL, getClass());
        ChFormatter formatter = new ChFormatter(oldFile, 0, oldFile.length(), config);
        Assertions.assertEquals(newFile, formatter.formatText(), "Formatted files are different");
    }

    @Test
    void testFormatSql() throws IOException {
        String newFile = TestUtils.readResource("new_ch_format_sql" + FILES_POSTFIX.SQL, getClass());
        String oldFile = TestUtils.readResource("old_ch" + FILES_POSTFIX.SQL, getClass());
        Assertions.assertEquals(newFile, ChFormatter.formatSql(oldFile));
    }
}