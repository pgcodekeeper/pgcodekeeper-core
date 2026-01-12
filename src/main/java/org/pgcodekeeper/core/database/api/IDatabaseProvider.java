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
package org.pgcodekeeper.core.database.api;

import org.antlr.v4.runtime.*;
import org.pgcodekeeper.core.database.api.jdbc.IJdbcConnector;
import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.base.schema.AbstractDatabase;
import org.pgcodekeeper.core.ignorelist.IgnoreSchemaList;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.settings.ISettings;

import java.io.IOException;

/**
 * Interface for DBMS
 */
public interface IDatabaseProvider {

    /**
     * @return short name of DBMS
     */
    String getName();

    /**
     * @return full name of DBMS
     */
    String getFullName();

    /**
     * @param stream - char stream
     * @return antlr lexer object for DBMS
     */
    Lexer getLexer(CharStream stream);

    /**
     * @param stream - token stream from lexer object for DBMS
     * @return antlr parser for DBMS
     */
    Parser getParser(CommonTokenStream stream);

    /**
     * @return error strategy for parser
     */
    ANTLRErrorStrategy getErrorHandler();

    /**
     * @param url full jdbc url
     * @return jdbc connector for DBMS
     * @see IJdbcConnector
     */
    IJdbcConnector getJdbcConnector(String url);

    /**
     * @param url full jdbc url
     * @param settings configuration settings for database comparison and migration operations
     * @param monitor progress monitor
     * @param ignoreSchemaList list of ignored schema
     * @return a database
     *
     * @see IDatabase
     * @see ISettings
     * @see IMonitor
     * @see IgnoreSchemaList
     */
    AbstractDatabase getDatabaseFromJdbc(String url, ISettings settings, IMonitor monitor, IgnoreSchemaList ignoreSchemaList)
            throws IOException, InterruptedException;
}
