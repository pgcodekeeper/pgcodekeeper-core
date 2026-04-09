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
package org.pgcodekeeper.core.database.base.parser;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

import org.antlr.v4.runtime.*;
import org.pgcodekeeper.core.database.base.parser.generated.*;
import org.pgcodekeeper.core.sql.KeywordCategory;

/**
 * Utility class for creating and managing ANTLR parser
 */
public class ParserUtils {

    public static final String SQL = ";";
    public static final String PARSED_OBJ_NAME = "fake string to clean parser cache";

    /**
     * Creates a parser for ignore list files.
     *
     * @param listFile path to the ignore list file
     * @return configured IgnoreListParser instance
     * @throws IOException if there's an error reading the file
     */
    public static IgnoreListParser createIgnoreListParser(Path listFile) throws IOException {
        String parsedObjectName = listFile.toString();
        var stream = CharStreams.fromPath(listFile);
        Lexer lexer = new IgnoreListLexer(stream);
        IgnoreListParser parser = new IgnoreListParser(new CommonTokenStream(lexer));
        addErrorListener(lexer, parser, parsedObjectName, null, 0, 0, 0);
        return parser;
    }

    public static void addErrorListener(Lexer lexer, Parser parser, String parsedObjectName,
                                         List<Object> errors, int offset, int lineOffset, int inLineOffset) {
        var listener = new CustomAntlrErrorListener(parsedObjectName, errors, offset, lineOffset, inLineOffset);
        lexer.removeErrorListeners();
        lexer.addErrorListener(listener);
        parser.removeErrorListeners();
        parser.addErrorListener(listener);
    }

    /**
     * Creates a dependencies list parser from the given file.
     *
     * @param depsFile the path to the dependencies list file
     * @return a configured parser instance
     * @throws IOException if the file cannot be read
     */
    public static DependenciesListParser createDependenciesListParser(Path depsFile) throws IOException {
        String parsedObjectName = depsFile.toString();
        var stream = CharStreams.fromPath(depsFile);
        Lexer lexer = new DependenciesListLexer(stream);
        DependenciesListParser parser = new DependenciesListParser(new CommonTokenStream(lexer));
        addErrorListener(lexer, parser, parsedObjectName, null, 0, 0, 0);
        return parser;
    }

    /**
     * Reads a range of token IDs from a vocabulary and populates a keyword map with
     * the specified category. This is a utility method for initializing keyword
     * maps based on lexer token definitions.
     *
     * @param vocab    the vocabulary containing token literal names
     * @param startId  the starting token (inclusive)
     * @param endId    the ending token(inclusive)
     * @param keywords the map to populate with keywords and their categories
     * @param type     the category to assign to all keywords in the specified range
     */
    public static void readKeywords(Vocabulary vocab, int startId, int endId, Map<String, KeywordCategory> keywords,
            KeywordCategory type) {
        for (int i = startId; i <= endId; i++) {
            String literal = vocab.getLiteralName(i);
            if (literal != null) {
                String clean = literal.substring(1, literal.length() - 1);
                keywords.put(clean.toLowerCase(Locale.ROOT), type);
            }
        }
    }

    private ParserUtils() {
    }
}
