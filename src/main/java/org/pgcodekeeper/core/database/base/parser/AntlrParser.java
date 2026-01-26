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
import java.util.List;

import org.antlr.v4.runtime.*;
import org.pgcodekeeper.core.database.base.parser.generated.IgnoreListLexer;
import org.pgcodekeeper.core.database.base.parser.generated.IgnoreListParser;

/**
 * Utility class for creating and managing ANTLR parser
 */
public final class AntlrParser {

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

    private AntlrParser() {
    }
}
