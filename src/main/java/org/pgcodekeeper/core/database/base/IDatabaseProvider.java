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
package org.pgcodekeeper.core.database.base;

import org.antlr.v4.runtime.*;
import org.pgcodekeeper.core.database.base.jdbc.IJdbcConnector;

/**
 * Interface for DBMS
 */
public interface IDatabaseProvider {

    /**
     * @return name of DBMS
     */
    String getDatabaseType();

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
     * Checks if a schema is a system schema for the given database type.
     *
     * @param schema the schema name to check
     * @return true if the schema is a system schema, false otherwise
     */
    boolean isSystemSchema(String schema);

    /**
     * @param url full jdbc url
     * @return jdbc connector for DBMS
     */
    IJdbcConnector getJdbcConnector(String url);
}
