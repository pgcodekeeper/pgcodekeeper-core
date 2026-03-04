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
package org.pgcodekeeper.core.database.ch.utils;

import java.util.*;

import org.antlr.v4.runtime.Vocabulary;
import org.pgcodekeeper.core.database.base.parser.ParserUtils;
import org.pgcodekeeper.core.database.ch.parser.generated.CHLexer;
import org.pgcodekeeper.core.sql.KeywordCategory;

public class ChKeyword {

    private static final Map<String, KeywordCategory> KEYWORDS;

    static {
        Map<String, KeywordCategory> keywords = new HashMap<>();
        Vocabulary vocab = CHLexer.VOCABULARY;

        ParserUtils.readKeywords(vocab, CHLexer.ACCESS, CHLexer.ZKPATH, keywords, KeywordCategory.UNRESERVED_KEYWORD);
        ParserUtils.readKeywords(vocab, CHLexer.ALL, CHLexer.WITH, keywords, KeywordCategory.RESERVED_KEYWORD);
        KEYWORDS = Collections.unmodifiableMap(keywords);
    }

    private ChKeyword() {}

    public static Map<String, KeywordCategory> getKeywords() {
        return KEYWORDS;
    }

    /**
     * Checks if a word is an allowed keyword.
     * <p>
     * A keyword is considered allowed if it's either not a keyword at all,
     * or it's an unreserved keyword.
     *
     * @param id the word to check
     * @return true if the word is not a keyword or is an unreserved keyword,
     *         false if it's a reserved keyword
     */
    public static boolean isKeyword(String id) {
        var keyword = KEYWORDS.get(id.toLowerCase(Locale.ROOT));
        return KeywordCategory.RESERVED_KEYWORD == keyword;
    }
}
