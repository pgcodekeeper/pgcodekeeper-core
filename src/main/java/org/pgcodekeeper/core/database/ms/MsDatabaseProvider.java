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
package org.pgcodekeeper.core.database.ms;

import org.antlr.v4.runtime.*;
import org.pgcodekeeper.core.database.api.IDatabaseProvider;
import org.pgcodekeeper.core.database.api.jdbc.IJdbcConnector;
import org.pgcodekeeper.core.database.ms.jdbc.MsJdbcConnector;
import org.pgcodekeeper.core.database.ms.loader.MsDumpLoader;
import org.pgcodekeeper.core.database.ms.loader.MsJdbcLoader;
import org.pgcodekeeper.core.database.ms.loader.MsProjectLoader;
import org.pgcodekeeper.core.database.ms.parser.MsCustomAntlrErrorStrategy;
import org.pgcodekeeper.core.database.ms.parser.generated.TSQLLexer;
import org.pgcodekeeper.core.database.ms.parser.generated.TSQLParser;
import org.pgcodekeeper.core.database.ms.schema.MsDatabase;
import org.pgcodekeeper.core.settings.DiffSettings;

import java.io.IOException;
import java.nio.file.Path;

public class MsDatabaseProvider implements IDatabaseProvider {

    @Override
    public String getName() {
        return "MS";
    }

    @Override
    public String getFullName() {
        return "MS SQL";
    }

    @Override
    public Lexer getLexer(CharStream stream) {
        return new TSQLLexer(stream);
    }

    @Override
    public Parser getParser(CommonTokenStream stream) {
        return new TSQLParser(stream);
    }

    @Override
    public ANTLRErrorStrategy getErrorHandler() {
        return new MsCustomAntlrErrorStrategy();
    }

    @Override
    public IJdbcConnector getJdbcConnector(String url) {
        return new MsJdbcConnector(url);
    }

    @Override
    public MsDatabase getDatabaseFromJdbc(String url, DiffSettings diffSettings)
            throws IOException, InterruptedException {
        return new MsJdbcLoader(getJdbcConnector(url), diffSettings).loadAndAnalyze();
    }

    @Override
    public MsDatabase getDatabaseFromDump(Path path, DiffSettings diffSettings)
            throws IOException, InterruptedException {
        return new MsDumpLoader(path, diffSettings).loadAndAnalyze();
    }

    @Override
    public MsDatabase getDatabaseFromProject(Path path, DiffSettings diffSettings)
            throws IOException, InterruptedException {
        return new MsProjectLoader(path, diffSettings).loadAndAnalyze();
    }
}
