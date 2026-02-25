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
package org.pgcodekeeper.core.database.pg.formatter;

import java.util.*;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.Lexer;
import org.pgcodekeeper.core.database.api.formatter.IFormatConfiguration;
import org.pgcodekeeper.core.database.base.formatter.*;
import org.pgcodekeeper.core.database.api.formatter.FormatItem;
import org.pgcodekeeper.core.database.base.parser.CodeUnitToken;
import org.pgcodekeeper.core.database.pg.parser.generated.*;
import org.pgcodekeeper.core.database.pg.parser.generated.SQLParser.*;
import org.pgcodekeeper.core.database.pg.parser.statement.PgParserAbstract;
import org.pgcodekeeper.core.database.pg.utils.PgDiffUtils;
import org.pgcodekeeper.core.utils.Pair;

/**
 * PostgreSQL-specific SQL formatter implementation.
 * Handles formatting of PostgreSQL functions and SQL statements with PostgreSQL-specific syntax rules.
 */
public class PgFormatter extends AbstractFormatter {

    /**
     * Constructs a new PostgreSQL formatter instance.
     *
     * @param source The source SQL text to format
     * @param offset Starting offset in the source text
     * @param length Length of text to format
     * @param config Formatting configuration options
     */
    public PgFormatter(String source, int offset, int length, IFormatConfiguration config) {
        super(source, offset, length, config);
    }

    @Override
    public List<FormatItem> getFormatItems() {
        List<FormatItem> changes = new ArrayList<>();

        Lexer lexer = new SQLLexer(CharStreams.fromString(source));
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        SQLParser parser = new SQLParser(tokenStream);

        SqlContext root = parser.sql();
        for (StatementContext st : root.statement()) {
            if (start <= ((CodeUnitToken) st.stop).getCodeUnitStop()
                    || ((CodeUnitToken) st.start).getCodeUnitStart() < stop) {
                fillChanges(st, tokenStream, changes);
            }
        }

        return changes;
    }

    private void fillChanges(StatementContext st, CommonTokenStream tokenStream,
                             List<FormatItem> changes) {
        Schema_statementContext schema = st.schema_statement();
        if (schema == null) {
            return;
        }

        Schema_createContext create = schema.schema_create();
        if (create == null) {
            return;
        }

        Create_function_statementContext function = create.create_function_statement();
        if (function == null) {
            return;
        }

        StatementFormatter sf;
        Function_bodyContext body = function.function_body();
        if (body != null) {
            sf = new PgStatementFormatter(start, stop, body, tokenStream, config);
        } else {
            String language = null;
            Function_defContext funcDef = null;
            for (Function_actions_commonContext action : function.function_actions_common()) {
                if (action.LANGUAGE() != null) {
                    language = action.lang_name.getText();
                } else if (action.AS() != null) {
                    funcDef = action.function_def();
                }
            }

            if (funcDef == null || funcDef.symbol != null || !PgDiffUtils.isValidLanguage(language)) {
                return;
            }

            Pair<String, Token> pair = PgParserAbstract.unquoteQuotedString(funcDef.definition);
            String definition = pair.getFirst();
            Token codeStart = pair.getSecond();

            sf = new PgStatementFormatter(start, stop, definition, ((CodeUnitToken) codeStart).getCodeUnitStart(),
                    language, config);
        }

        sf.format();
        changes.addAll(sf.getChanges());
    }
}