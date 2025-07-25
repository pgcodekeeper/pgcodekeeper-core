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
package org.pgcodekeeper.core.parsers;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.pgcodekeeper.core.parsers.antlr.AntlrParser;
import org.pgcodekeeper.core.sql.Keyword;
import org.pgcodekeeper.core.sql.Keyword.LabelCategory;

class KeywordAliasParseTest {

    @ParameterizedTest
    @EnumSource(LabelCategory.class)
    void testAliases(LabelCategory labelCategory) {

        String as = labelCategory == LabelCategory.AS_LABEL ? "AS " : "";
        StringBuilder sb = new StringBuilder("SELECT ");
        Keyword.KEYWORDS.values().stream()
            .filter(k -> k.getLabelCategory() == labelCategory)
            .forEach(k -> sb.append("\nCol ").append(as).append(k.getKeyword()).append(','));

        sb.setLength(sb.length() - 1);
        sb.append(';');
        String select = sb.toString();

        List<Object> errors = new ArrayList<>();
        AntlrParser.createSQLParser(select, labelCategory.name(), errors).sql();
        Assertions.assertTrue(errors.isEmpty(), "KeywordAliasParseTest: " + labelCategory + " - ANTLR Error");
    }
}
