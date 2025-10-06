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
package org.pgcodekeeper.core.formatter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.pgcodekeeper.core.DatabaseType;
import org.pgcodekeeper.core.FILES_POSTFIX;
import org.pgcodekeeper.core.formatter.FormatConfiguration.IndentType;
import org.pgcodekeeper.core.utils.FileUtils;

import java.io.IOException;

class FormatterTest {

    /**
     * Testing default parameters
     */
    @Test
    void testDefault() throws IOException {
        FormatConfiguration config = new FormatConfiguration();
        config.setAddWhitespaceAfterOp(true);
        config.setAddWhitespaceBeforeOp(true);
        config.setIndentSize(2);
        config.setIndentType(IndentType.WHITESPACE);
        config.setRemoveTrailingWhitespace(true);

        String newFile = "new_Default_config";
        String oldFile = "old_Default_config";
        testFormatter(oldFile, newFile, config, DatabaseType.PG);
    }

    /**
     * Testing default parameters
     */
    @Test
    void testBroken() throws IOException {
        FormatConfiguration config = new FormatConfiguration();
        config.setAddWhitespaceAfterOp(true);
        config.setAddWhitespaceBeforeOp(true);
        config.setIndentSize(2);
        config.setIndentType(IndentType.WHITESPACE);
        config.setRemoveTrailingWhitespace(true);

        String newFile = "new_Broken";
        String oldFile = "old_Broken";
        testFormatter(oldFile, newFile, config, DatabaseType.PG);
    }

    @Test
    void testRemoveTrailingWhitespace() throws IOException {
        FormatConfiguration config = new FormatConfiguration();
        config.setRemoveTrailingWhitespace(true);

        String newFile = "new_RemoveTrailingWhitespace";
        String oldFile = "old_RemoveTrailingWhitespace";
        testFormatter(oldFile, newFile, config, DatabaseType.PG);
    }

    @Test
    void testAddWhitespaceAfterOp() throws IOException {
        FormatConfiguration config = new FormatConfiguration();
        config.setAddWhitespaceAfterOp(true);

        String newFile = "new_AddWhitespaceAfterOp";
        String oldFile = "old_AddWhitespaceOp";
        testFormatter(oldFile, newFile, config, DatabaseType.PG);
    }

    @Test
    void testAddWhitespaceBeforeOp() throws IOException {
        FormatConfiguration config = new FormatConfiguration();
        config.setAddWhitespaceBeforeOp(true);

        String newFile = "new_AddWhitespaceBeforeOp";
        String oldFile = "old_AddWhitespaceOp";
        testFormatter(oldFile, newFile, config, DatabaseType.PG);
    }

    @Test
    void testAddSpacesForTabs() throws IOException {
        FormatConfiguration config = new FormatConfiguration();
        config.setIndentType(IndentType.WHITESPACE);
        config.setIndentSize(8);

        String newFile = "new_SpacesForTabs";
        String oldFile = "old_SpacesForTabs";
        testFormatter(oldFile, newFile, config, DatabaseType.PG);
    }

    @Test
    void testIndentSize() throws IOException {
        FormatConfiguration config = new FormatConfiguration();
        config.setIndentType(IndentType.WHITESPACE);
        config.setIndentSize(8);

        String newFile = "new_IndentSize";
        String oldFile = "old";
        testFormatter(oldFile, newFile, config, DatabaseType.PG);
    }

    @Test
    void testIndentType() throws IOException {
        FormatConfiguration config = new FormatConfiguration();
        config.setIndentType(IndentType.TAB);
        config.setIndentSize(1);

        String newFile = "new_indent_type";
        String oldFile = "old";
        testFormatter(oldFile, newFile, config, DatabaseType.PG);
    }

    @Test
    void testIndentTypeTab() throws IOException {
        FormatConfiguration config = new FormatConfiguration();
        config.setIndentType(IndentType.TAB);
        config.setIndentSize(2);

        String newFile = "new_IndentTypeTab";
        String oldFile = "old";
        testFormatter(oldFile, newFile, config, DatabaseType.PG);
    }

    @Test
    void testCh() throws IOException {
        FormatConfiguration config = new FormatConfiguration();
        config.setIndentSize(2);
        config.setAddWhitespaceAfterOp(true);
        config.setAddWhitespaceBeforeOp(true);
        config.setIndentType(IndentType.WHITESPACE);
        config.setRemoveTrailingWhitespace(true);

        String newFile = "new_ch";
        String oldFile = "old_ch";
        testFormatter(oldFile, newFile, config, DatabaseType.CH);
    }

    @Test
    void testFormatSql() throws IOException {
        String newFile = getFileContent("new_ch_format_sql" + FILES_POSTFIX.SQL);
        String oldFile = getFileContent("old_ch" + FILES_POSTFIX.SQL);
        Assertions.assertEquals(newFile, FileFormatter.formatSql(oldFile, DatabaseType.CH));
    }

    private void testFormatter(String oldFileName, String newFileName, FormatConfiguration config, DatabaseType dbType)
            throws IOException {
        String newFile = getFileContent(newFileName + FILES_POSTFIX.SQL);
        String oldFile = getFileContent(oldFileName + FILES_POSTFIX.SQL);
        FileFormatter fileFormatter = new FileFormatter(oldFile, 0, oldFile.length(), config, dbType);
        Assertions.assertEquals(newFile, fileFormatter.formatText(), "Formatted files are different");
    }

    private String getFileContent(String fileName) throws IOException {
        return FileUtils.readResource(FormatterTest.class, fileName);
    }
}