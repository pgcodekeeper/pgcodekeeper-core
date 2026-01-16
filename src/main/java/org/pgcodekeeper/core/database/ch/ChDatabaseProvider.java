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
package org.pgcodekeeper.core.database.ch;

import java.io.IOException;

import org.antlr.v4.runtime.ANTLRErrorStrategy;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Parser;
import org.pgcodekeeper.core.database.api.IDatabaseProvider;
import org.pgcodekeeper.core.database.api.jdbc.IJdbcConnector;
import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.ch.jdbc.ChJdbcConnector;
import org.pgcodekeeper.core.database.ch.loader.ChJdbcLoader;
import org.pgcodekeeper.core.database.ch.parser.ChCustomAntlrErrorStrategy;
import org.pgcodekeeper.core.database.ch.parser.generated.*;
import org.pgcodekeeper.core.database.ch.schema.ChDatabase;
import org.pgcodekeeper.core.ignorelist.IgnoreSchemaList;
import org.pgcodekeeper.core.loader.FullAnalyze;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.settings.ISettings;

import java.nio.file.Path;

public class ChDatabaseProvider implements IDatabaseProvider {

    @Override
    public String getName() {
        return "CH";
    }

    @Override
    public String getFullName() {
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
        return new ChCustomAntlrErrorStrategy();
    }

    @Override
    public IJdbcConnector getJdbcConnector(String url) {
        return new ChJdbcConnector(url);
    }

    @Override
    public ChDatabase getDatabaseFromJdbc(String url, ISettings settings, IMonitor monitor, IgnoreSchemaList ignoreSchemaList) throws IOException, InterruptedException {
        var loader = new ChJdbcLoader(getJdbcConnector(url), settings, monitor, ignoreSchemaList);
        var db = loader.load();
        FullAnalyze.fullAnalyze(db, loader.getErrors());
        return db;
    }

    @Override
    public IDatabase getDatabaseFromDump(Path path, ISettings settings, IMonitor monitor) {
        return null;
    }
}
