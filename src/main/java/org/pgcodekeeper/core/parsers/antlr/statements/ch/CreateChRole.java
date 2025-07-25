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
package org.pgcodekeeper.core.parsers.antlr.statements.ch;

import org.pgcodekeeper.core.parsers.antlr.generated.CHParser.Create_role_stmtContext;
import org.pgcodekeeper.core.schema.ch.ChDatabase;
import org.pgcodekeeper.core.schema.ch.ChRole;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.List;

/**
 * Parser for ClickHouse CREATE ROLE statements.
 * Handles role creation including storage type configuration and support for multiple roles in one statement.
 */
public final class CreateChRole extends ChParserAbstract {

    private final Create_role_stmtContext ctx;

    /**
     * Creates a parser for ClickHouse CREATE ROLE statements.
     *
     * @param ctx      the ANTLR parse tree context for the CREATE ROLE statement
     * @param db       the ClickHouse database schema being processed
     * @param settings parsing configuration settings
     */
    public CreateChRole(Create_role_stmtContext ctx, ChDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        for (var roleNameWithCluster : ctx.name_with_cluster()) {
            var roleNameCtx = roleNameWithCluster.identifier();
            ChRole role = new ChRole(roleNameCtx.getText());
            var storageType = ctx.identifier();
            if (storageType != null) {
                role.setStorageType(storageType.getText());
            }
            addSafe(db, role, List.of(roleNameCtx));
        }
    }

    @Override
    protected String getStmtAction() {
        return "CREATE ROLE";
    }
}
