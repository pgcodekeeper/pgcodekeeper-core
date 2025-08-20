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
package org.pgcodekeeper.core.parsers.antlr.pg.verification;

import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.parsers.antlr.base.AntlrError;
import org.pgcodekeeper.core.parsers.antlr.base.CodeUnitToken;
import org.pgcodekeeper.core.parsers.antlr.base.ErrorTypes;
import org.pgcodekeeper.core.parsers.antlr.base.verification.IVerification;
import org.pgcodekeeper.core.parsers.antlr.base.verification.VerificationProperties;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.IdentifierContext;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.Role_name_with_groupContext;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.Rule_commonContext;

import java.util.List;

/**
 * Verification implementation for GRANT/REVOKE statements.
 * Checks GRANT statements against configured rules to ensure they comply
 * with security policies, including restrictions on denied users and roles.
 */
public final class VerificationGrant implements IVerification {

    private final Rule_commonContext ruleCtx;
    private final VerificationProperties rules;
    private final String fileName;
    private final List<Object> errors;

    VerificationGrant(Rule_commonContext ruleCtx, VerificationProperties rules, String fileName,
                      List<Object> errors) {
        this.ruleCtx = ruleCtx;
        this.rules = rules;
        this.fileName = fileName;
        this.errors = errors;
    }

    @Override
    public void verify() {
        var deniedUsers = rules.getDeniedUsers();
        if (deniedUsers.isEmpty() || ruleCtx.REVOKE() != null || ruleCtx.other_rules() != null) {
            return;
        }

        for (Role_name_with_groupContext roleCtx : ruleCtx.roles_names().role_name_with_group()) {
            // skip CURRENT_USER and SESSION_USER
            IdentifierContext user = roleCtx.user_name().identifier();
            if (user == null) {
                continue;
            }
            String role = user.getText();
            if (deniedUsers.contains(role)) {
                AntlrError err = new AntlrError(user.getStart(), fileName, ruleCtx.getStop().getLine(),
                        ((CodeUnitToken) ruleCtx.getStart()).getCodeUnitPositionInLine(),
                        Messages.VerificationGrant_denied_grant + role,
                        ErrorTypes.VERIFICATIONERROR);
                errors.add(err);
            }
        }
    }
}
