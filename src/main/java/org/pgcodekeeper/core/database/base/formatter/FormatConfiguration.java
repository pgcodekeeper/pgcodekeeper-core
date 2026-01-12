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
package org.pgcodekeeper.core.database.base.formatter;

import org.pgcodekeeper.core.database.api.formatter.IndentType;
import org.pgcodekeeper.core.database.api.formatter.IFormatConfiguration;

import java.util.Arrays;

/**
 * Configuration class for SQL formatting options.
 * Controls various aspects of SQL code formatting including indentation,
 * whitespace handling, and operator spacing.
 */
public class FormatConfiguration implements IFormatConfiguration {

    private boolean addWhitespaceBeforeOp;
    private boolean addWhitespaceAfterOp;
    private boolean removeTrailingWhitespace;

    private IndentType indentType = IndentType.DISABLE;
    private int indentSize;

    public void setAddWhitespaceBeforeOp(boolean addWhitespaceBeforeOp) {
        this.addWhitespaceBeforeOp = addWhitespaceBeforeOp;
    }

    public void setAddWhitespaceAfterOp(boolean addWhitespaceAfterOp) {
        this.addWhitespaceAfterOp = addWhitespaceAfterOp;
    }

    public void setRemoveTrailingWhitespace(boolean removeTrailingWhitespace) {
        this.removeTrailingWhitespace = removeTrailingWhitespace;
    }

    public void setIndentSize(int indentSize) {
        this.indentSize = indentSize;
    }

    public boolean isAddWhitespaceAfterOp() {
        return addWhitespaceAfterOp;
    }

    public boolean isAddWhitespaceBeforeOp() {
        return addWhitespaceBeforeOp;
    }

    public boolean isRemoveTrailingWhitespace() {
        return removeTrailingWhitespace;
    }

    public int getIndentSize() {
        return indentSize;
    }

    public IndentType getIndentType() {
        return indentType;
    }

    public void setIndentType(IndentType indentType) {
        this.indentType = indentType;
    }

    public String createIndent(int length) {
        return createIndent(length, getIndentType() == IndentType.TAB ? '\t' : ' ');
    }

    /**
     * Creates an indentation string with specified character.
     *
     * @param length     desired length of indentation
     * @param indentChar character to use for indentation
     * @return indentation string
     */
    public static String createIndent(int length, char indentChar) {
        if (length <= 0) {
            return "";
        }

        char [] chars  = new char[length];
        Arrays.fill(chars, indentChar);

        return new String(chars);
    }

    /**
     * Creates a copy of this configuration.
     *
     * @return new FormatConfiguration with same settings
     */
    public FormatConfiguration copy() {
        FormatConfiguration config = new FormatConfiguration();
        config.addWhitespaceBeforeOp = isAddWhitespaceBeforeOp();
        config.addWhitespaceAfterOp = isAddWhitespaceAfterOp();
        config.removeTrailingWhitespace = isRemoveTrailingWhitespace();
        config.indentType = getIndentType();
        config.indentSize = getIndentSize();
        return config;
    }

    public static FormatConfiguration getDefaultConfig() {
        var config = new FormatConfiguration();
        config.indentSize = 2;
        config.indentType = IndentType.WHITESPACE;
        config.addWhitespaceAfterOp = true;
        config.addWhitespaceBeforeOp = true;
        config.removeTrailingWhitespace = true;
        return config;
    }
}
