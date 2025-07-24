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
package org.pgcodekeeper.core.parsers.antlr.verification;

import org.antlr.v4.runtime.CommonTokenStream;
import org.pgcodekeeper.core.parsers.antlr.AntlrContextProcessor.SqlContextProcessor;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.Schema_createContext;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.Schema_statementContext;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.SqlContext;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.StatementContext;

import java.util.List;

/**
 * Parser listener for SQL verification processes.
 * Processes SQL contexts to identify and verify various statement types
 * including functions and GRANT statements according to configured rules.
 */
public class VerificationParserListener implements SqlContextProcessor {

    private final VerificationProperties rules;
    private final String fileName;
    private final List<Object> errors;

    /**
     * Creates a new verification parser listener.
     *
     * @param rules    verification rules and properties to apply
     * @param fileName the name of the file being verified
     * @param errors   list to collect verification errors
     */
    public VerificationParserListener(VerificationProperties rules, String fileName, List<Object> errors) {
        this.rules = rules;
        this.fileName = fileName;
        this.errors = errors;
    }

    @Override
    public void process(SqlContext rootCtx, CommonTokenStream stream) {
        for (StatementContext s : rootCtx.statement()) {
            statement(s);
        }
    }

    private void statement(StatementContext statement) {
        Schema_statementContext schema = statement.schema_statement();
        if (schema == null) {
            return;
        }

        Schema_createContext create = schema.schema_create();
        if (create == null) {
            return;
        }

        IVerification verification = null;
        if (create.create_function_statement() != null) {
            verification = new VerificationFunction(create, rules, fileName, errors);
        } else if (create.rule_common() != null) {
            verification = new VerificationGrant(create.rule_common(), rules, fileName, errors);
        }

        if (verification != null) {
            verification.verify();
        }
    }
}