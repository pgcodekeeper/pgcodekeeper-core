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
package org.pgcodekeeper.core.database.ch;

import org.antlr.v4.runtime.*;
import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.database.api.IDatabaseProvider;
import org.pgcodekeeper.core.database.api.jdbc.IJdbcConnector;
import org.pgcodekeeper.core.database.ch.jdbc.ChJdbcConnector;
import org.pgcodekeeper.core.parsers.antlr.ch.CustomChSQLAntlrErrorStrategy;
import org.pgcodekeeper.core.parsers.antlr.ch.generated.CHLexer;
import org.pgcodekeeper.core.parsers.antlr.ch.generated.CHParser;

public class ChDatabaseProvider implements IDatabaseProvider {

    @Override
    public String getDatabaseType() {
        return "ClickHouse";
    }

    @Override
    public Lexer getLexer(CharStream stream) {
        return new CHLexer(stream);
    }

    @Override
    public Parser getParser(CommonTokenStream stream) {
        return new CHParser(stream);
    }

    @Override
    public ANTLRErrorStrategy getErrorHandler() {
        return new CustomChSQLAntlrErrorStrategy();
    }

    @Override
    public boolean isSystemSchema(String schema) {
        return Consts.SYSTEM.equalsIgnoreCase(schema)
                || Consts.INFORMATION_SCHEMA.equalsIgnoreCase(schema);
    }

    @Override
    public IJdbcConnector getJdbcConnector(String url) {
        return new ChJdbcConnector(url);
    }
}
