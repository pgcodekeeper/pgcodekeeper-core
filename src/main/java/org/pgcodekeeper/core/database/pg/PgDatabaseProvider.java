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
package org.pgcodekeeper.core.database.pg;

import java.io.IOException;

import org.antlr.v4.runtime.ANTLRErrorStrategy;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Parser;
import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.database.api.IDatabaseProvider;
import org.pgcodekeeper.core.database.api.jdbc.IJdbcConnector;
import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.pg.jdbc.PgJdbcConnector;
import org.pgcodekeeper.core.database.pg.loader.PgDumpLoader;
import org.pgcodekeeper.core.database.pg.loader.PgJdbcLoader;
import org.pgcodekeeper.core.database.pg.parser.PgCustomAntlrErrorStrategy;
import org.pgcodekeeper.core.database.pg.parser.generated.*;
import org.pgcodekeeper.core.database.pg.schema.PgDatabase;
import org.pgcodekeeper.core.ignorelist.IgnoreSchemaList;
import org.pgcodekeeper.core.loader.FullAnalyze;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.settings.ISettings;

import java.nio.file.Path;

public class PgDatabaseProvider implements IDatabaseProvider {

    @Override
    public String getName() {
        return "PG";
    }

    @Override
    public String getFullName() {
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
        return new PgCustomAntlrErrorStrategy();
    }

    @Override
    public IJdbcConnector getJdbcConnector(String url) {
        return new PgJdbcConnector(url);
    }

    @Override
    public PgDatabase getDatabaseFromJdbc(String url, ISettings settings, IMonitor monitor, IgnoreSchemaList ignoreSchemaList)
            throws IOException, InterruptedException {
        String timezone = settings.getTimeZone() == null ? Consts.UTC : settings.getTimeZone();
        var loader = new PgJdbcLoader(getJdbcConnector(url), timezone, settings, monitor, ignoreSchemaList);
        var db = loader.load();
        FullAnalyze.fullAnalyze(db, loader.getErrors());
        return db;
    }

    @Override
    public IDatabase getDatabaseFromDump(Path path, ISettings settings, IMonitor monitor)
            throws IOException, InterruptedException {
        var loader = new PgDumpLoader(path, settings, monitor);
        var db = loader.load();
        FullAnalyze.fullAnalyze(db, loader.getErrors());
        return db;
    }
}
