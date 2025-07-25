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

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.parsers.antlr.AntlrError;
import org.pgcodekeeper.core.parsers.antlr.CodeUnitToken;
import org.pgcodekeeper.core.parsers.antlr.ErrorTypes;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.*;

import java.util.List;

/**
 * Parse tree listener for function body verification.
 * Walks through function parse trees to count methods and perform
 * various code quality checks on function implementations.
 */
public class VerificationFunctionTreeListener implements ParseTreeListener {

    private final String fileName;
    private final List<Object> errors;
    private final VerificationProperties rules;
    private final Token token;
    private int methodCount = 0;

    /**
     * Creates a new function tree listener for verification.
     *
     * @param fileName the name of the file being verified
     * @param rules    verification rules and properties to apply
     * @param errors   list to collect verification errors
     * @param token    the token representing the function definition
     */
    public VerificationFunctionTreeListener(String fileName, VerificationProperties rules, List<Object> errors,
                                            Token token) {
        this.fileName = fileName;
        this.rules = rules;
        this.errors = errors;
        this.token = token;
    }

    /**
     * Returns the count of methods found during tree traversal.
     *
     * @return the number of methods counted in the function
     */
    public int getMethodCount() {
        return methodCount;
    }

    @Override
    public void visitTerminal(TerminalNode node) {
        //no imp
    }

    @Override
    public void visitErrorNode(ErrorNode node) {
        //no imp
    }

    @Override
    public void enterEveryRule(ParserRuleContext ctx) {
        //no imp
    }

    @Override
    public void exitEveryRule(ParserRuleContext ctx) {
        if (rules.getMethodCount() > 0) {
            countMethodCount(ctx);
        }

        if (rules.isCheckCaseWithElse()) {
            if (ctx instanceof Case_statementContext caseCtx) {
                if (caseCtx.ELSE() == null) {
                    verifyCase(ctx);
                }
                return;
            }

            if (ctx instanceof Case_expressionContext caseCtx) {
                if (caseCtx.ELSE() == null) {
                    verifyCase(ctx);
                }
                return;
            }
        }

        if (ctx instanceof Create_table_statementContext tableCtx && rules.isCheckTempTable()) {
            if (tableCtx.TEMP() == null && tableCtx.TEMPORARY() == null) {
                String msg = Messages.VerificationFunctionTreeListener_temp_table;
                AntlrError err = new AntlrError(fileName, tableCtx.getStart().getLine(),
                        ((CodeUnitToken) tableCtx.getStart()).getCodeUnitPositionInLine(), msg, ErrorTypes.VERIFICATIONERROR)
                        .copyWithOffset(((CodeUnitToken) token).getCodeUnitStart(),
                                token.getLine() - 1, ((CodeUnitToken) token).getCodeUnitPositionInLine());
                errors.add(err);
            }
            return;
        }

        if (ctx instanceof Table_column_defContext col && rules.isCheckQuotesInTableColumn()) {
            var column = col.table_column_definition();
            if (column == null) {
                return;
            }

            var identifier = column.identifier();
            if (identifier == null) {
                return;
            }
            var id = identifier.id_token();
            if (id == null) {
                return;
            }

            if (id.QuotedIdentifier() != null) {
                String msg = Messages.VerificationFunctionTreeListener_quotation_marks;
                CodeUnitToken cuToken = (CodeUnitToken) token;
                AntlrError err = new AntlrError(fileName, id.getStart().getLine(),
                        ((CodeUnitToken) id.getStart()).getCodeUnitPositionInLine(), msg, ErrorTypes.VERIFICATIONERROR)
                        .copyWithOffset(cuToken.getCodeUnitStart(), cuToken.getLine() - 1,
                                cuToken.getCodeUnitPositionInLine());
                errors.add(err);
            }
        }
    }

    //for checking NcssMethodCountRule we get methodCount
    private void countMethodCount(ParserRuleContext ctx) {
        if (ctx instanceof Return_stmtContext
                || ctx instanceof Message_statementContext
                || ctx instanceof Data_statementContext
                || ctx instanceof Truncate_stmtContext
                || ctx instanceof Base_statementContext
                || ctx instanceof With_clauseContext
                || ctx instanceof Execute_statementContext
                || ctx instanceof Transaction_statementContext
                || ctx instanceof Case_expressionContext
                || ctx instanceof Set_statementContext
        ) {
            methodCount++;
        } else if (ctx instanceof Control_statementContext controlCtx) {
            if (controlCtx.CALL() != null) {
                methodCount++;
            }
        } else if (ctx instanceof Exception_statementContext exceptionCtx) {
            methodCount++;
            methodCount += exceptionCtx.function_statements().size();
        } else if (ctx instanceof Loop_statementContext loopCtx) {
            methodCount += 3;
            var loopStartCtx = loopCtx.loop_start();
            if (loopStartCtx != null) {
                if (loopStartCtx.WHILE() != null
                        || loopCtx.EXIT() != null
                        || loopCtx.CONTINUE() != null
                        || loopCtx.WHEN() != null) {
                    methodCount++;
                }
                if (loopStartCtx.IN() != null) {
                    methodCount += loopStartCtx.vex() != null ? loopStartCtx.vex().size() : 1;
                }
            }
        } else if (ctx instanceof If_statementContext ifCtx) {
            methodCount++;
            methodCount += ifCtx.ELSEIF().size();
            methodCount += ifCtx.ELSIF().size();
            if (ifCtx.ELSE() != null) {
                methodCount++;
            }
        } else if (ctx instanceof Case_statementContext caseCtx) {
            methodCount++;
            methodCount += caseCtx.vex().size();
        }
    }

    private void verifyCase(ParserRuleContext caseCtx) {
        String msg = Messages.VerificationFunctionTreeListener_case_block;
        AntlrError err = new AntlrError(fileName, caseCtx.getStop().getLine(),
                ((CodeUnitToken) caseCtx.getStart()).getCodeUnitPositionInLine(), msg, ErrorTypes.VERIFICATIONERROR);
        errors.add(err);
    }
}
