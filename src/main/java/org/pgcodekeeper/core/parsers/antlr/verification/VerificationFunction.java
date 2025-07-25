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
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.pgcodekeeper.core.PgDiffUtils;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.parsers.antlr.*;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.*;
import org.pgcodekeeper.core.parsers.antlr.statements.ParserAbstract;
import org.pgcodekeeper.core.parsers.antlr.statements.pg.PgParserAbstract;
import org.pgcodekeeper.core.schema.ArgMode;
import org.pgcodekeeper.core.utils.Pair;

import java.text.MessageFormat;
import java.util.List;
import java.util.Locale;

/**
 * Verification implementation for PostgreSQL function statements.
 * Analyzes function definitions to ensure they comply with coding standards
 * and best practices, including complexity checks, parameter validation,
 * and code style verification.
 */
public class VerificationFunction implements IVerification {
    private final Schema_createContext createCtx;
    private final Create_function_statementContext ctx;
    private final SconstContext definitionCtx;
    private final VerificationProperties rules;
    private final List<Object> errors;
    private final String fileName;
    private String language;
    private int methodCount;

    /**
     * Creates a new function verification instance.
     *
     * @param createCtx the schema creation context containing the function
     * @param rules     verification rules and properties to apply
     * @param fileName  the name of the file being verified
     * @param errors    list to collect verification errors
     */
    public VerificationFunction(Schema_createContext createCtx, VerificationProperties rules,
                                String fileName, List<Object> errors) {
        this.createCtx = createCtx;
        this.ctx = createCtx.create_function_statement();
        this.rules = rules;
        this.fileName = fileName;
        this.errors = errors;
        this.definitionCtx = getFunctionDefinition();
    }

    @Override
    public void verify() {
        verifyFuncLength();
        verifyFuncParams();
        verifyFunctionBody();
        verifyMethodCount();
        verifyIndents();
        verifyDollarStyle();
    }

    /**
     * checking max length;
     */
    private void verifyFuncLength() {
        var ruleLength = rules.getMaxFunctionLenght();
        var funcLength = ctx.getStop().getLine();
        if (ruleLength > 0 && funcLength > ruleLength) {
            addError(MessageFormat.format(Messages.VerificationFunction_function_length, funcLength, ruleLength));
        }
    }

    /**
     * checking NCSS rule;
     */
    private void verifyMethodCount() {
        int ruleMethodCount = rules.getMethodCount();
        if (ruleMethodCount > 0 && methodCount > ruleMethodCount) {
            addError(MessageFormat.format(Messages.VerificationFunction_ncss, methodCount, ruleMethodCount));
        }
    }

    /**
     * checking the number of parameters in a function
     */
    private void verifyFuncParams() {
        int limitParams = rules.getMaxFunctionParams();
        if (limitParams < 0) {
            return;
        }

        var countParams = ctx.function_parameters().function_args().function_arguments().stream().
                filter(arg -> ParserAbstract.parseArgMode(arg.argmode()) != ArgMode.OUT).count();
        if (countParams > limitParams) {
            addError(MessageFormat.format(Messages.VerificationFunction_function_params, countParams, limitParams));
        }
    }

    /**
     * checking the case block rule & rule for creating a table. Walk on function body
     */
    private void verifyFunctionBody() {
        Function_bodyContext body = ctx.function_body();
        if (body != null) {
            runVerifyListener(body, null);
            return;
        }

        if (definitionCtx == null) {
            return;
        }

        Pair<String, Token> pair = PgParserAbstract.unquoteQuotedString(definitionCtx);
        var parser = AntlrParser.createSQLParser(pair.getFirst(), fileName, errors);

        ParserRuleContext contex;
        if ("SQL".equalsIgnoreCase(language)) { //$NON-NLS-1$
            contex = parser.sql();
        } else {
            AntlrUtils.removeIntoStatements(parser);
            contex = parser.plpgsql_function();
        }
        runVerifyListener(contex, pair.getSecond());
    }

    /**
     * checking cyclomatic complexity
     * stylistic check of indents
     */
    private void verifyIndents() {
        Function_bodyContext body = ctx.function_body();
        if (body != null) {
            var parser = AntlrParser.createSQLParser(ParserAbstract.getFullCtxText(createCtx), fileName, errors);
            parser.sql();
            CommonTokenStream tokenStream = (CommonTokenStream) parser.getTokenStream();

            new VerificationIndents(ctx.getStart(), body, tokenStream, fileName, errors, rules).verify();
        } else if (definitionCtx != null) {
            Pair<String, Token> pair = PgParserAbstract.unquoteQuotedString(definitionCtx);
            String definition = pair.getFirst();
            Token codeStart = pair.getSecond();

            new VerificationIndents(codeStart, definition, language, fileName, errors, rules).verify();
        }
    }

    /**
     * Checking DollarStyle rule Dollar must begin with a new line and front of him no contains spaces.
     */
    private void verifyDollarStyle() {
        var allowedFunctionStart = rules.getAllowedFunctionStart();
        if (allowedFunctionStart.isEmpty() || definitionCtx == null) {
            return;
        }

        var beginToken = definitionCtx.getStart();
        if (!allowedFunctionStart.contains(beginToken.getText().toLowerCase(Locale.ROOT))) {
            addError(MessageFormat.format(Messages.VerificationFunction_body_start, allowedFunctionStart), beginToken);
        }
    }

    private void addError(String msg) {
        addError(msg, 1, 0);
    }

    private void addError(String msg, Token token) {
        addError(msg, token.getLine(), ((CodeUnitToken) token).getCodeUnitPositionInLine());
    }

    private void addError(String msg, int line, int position) {
        AntlrError err = new AntlrError(fileName, line, position, msg, ErrorTypes.VERIFICATIONERROR);
        errors.add(err);
    }

    private SconstContext getFunctionDefinition() {
        Function_defContext funcDef = null;
        for (Function_actions_commonContext action : ctx.function_actions_common()) {
            if (action.LANGUAGE() != null) {
                language = action.lang_name.getText();
            } else if (action.AS() != null) {
                funcDef = action.function_def();
            }
        }

        if (funcDef == null || funcDef.symbol != null || !PgDiffUtils.isValidLanguage(language)) {
            return null;
        }

        return funcDef.definition;
    }

    private void runVerifyListener(ParserRuleContext definition, Token token) {
        var listener = new VerificationFunctionTreeListener(fileName, rules, errors, token);
        ParseTreeWalker.DEFAULT.walk(listener, definition);
        this.methodCount = listener.getMethodCount();
    }
}
