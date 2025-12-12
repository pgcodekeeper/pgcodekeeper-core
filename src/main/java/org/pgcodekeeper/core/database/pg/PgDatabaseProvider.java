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
package org.pgcodekeeper.core.database.pg;

import org.antlr.v4.runtime.*;
import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.database.api.IDatabaseProvider;
import org.pgcodekeeper.core.database.api.jdbc.IJdbcConnector;
import org.pgcodekeeper.core.database.pg.jdbc.PgJdbcConnector;
import org.pgcodekeeper.core.parsers.antlr.pg.CustomSQLAntlrErrorStrategy;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLLexer;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser;

public class PgDatabaseProvider implements IDatabaseProvider {

    @Override
    public String getDatabaseType() {
        return "PostgreSQL";
    }

    @Override
    public Lexer getLexer(CharStream stream) {
        return new SQLLexer(stream);
    }

    @Override
    public Parser getParser(CommonTokenStream stream) {
        return new SQLParser(stream);
    }

    @Override
    public ANTLRErrorStrategy getErrorHandler() {
        return new CustomSQLAntlrErrorStrategy();
    }

    @Override
    public boolean isSystemSchema(String schema) {
        return Consts.PG_CATALOG.equalsIgnoreCase(schema)
                || Consts.INFORMATION_SCHEMA.equalsIgnoreCase(schema);
    }

    @Override
    public IJdbcConnector getJdbcConnector(String url) {
        return new PgJdbcConnector(url);
    }
}
