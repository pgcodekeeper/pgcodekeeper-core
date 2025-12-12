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
package org.pgcodekeeper.core.parsers.antlr.ch.statement;

import org.pgcodekeeper.core.ChDiffUtils;
import org.pgcodekeeper.core.database.api.schema.DatabaseType;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.ch.generated.CHParser.Columns_permissionsContext;
import org.pgcodekeeper.core.parsers.antlr.ch.generated.CHParser.IdentifierContext;
import org.pgcodekeeper.core.parsers.antlr.ch.generated.CHParser.Privilegy_stmtContext;
import org.pgcodekeeper.core.parsers.antlr.base.statement.ParserAbstract;
import org.pgcodekeeper.core.database.api.schema.ObjectPrivilege;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.database.base.schema.StatementOverride;
import org.pgcodekeeper.core.database.ch.schema.ChDatabase;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.AbstractMap.SimpleEntry;
import java.util.*;
import java.util.Map.Entry;

/**
 * Parser for ClickHouse GRANT and REVOKE privilege statements.
 * Handles privilege assignment and revocation for users and roles on database objects,
 * including support for column-level privileges and grant options.
 */
public final class GrantChPrivilege extends ChParserAbstract {

    private final Privilegy_stmtContext ctx;
    private final String state;
    private final Map<AbstractStatement, StatementOverride> overrides;

    /**
     * Creates a parser for ClickHouse GRANT/REVOKE privilege statements.
     *
     * @param ctx       the ANTLR parse tree context for the GRANT/REVOKE statement
     * @param db        the ClickHouse database schema being processed
     * @param overrides map of statement overrides for privilege modifications
     * @param settings  parsing configuration settings
     */
    public GrantChPrivilege(Privilegy_stmtContext ctx, ChDatabase db, Map<AbstractStatement, StatementOverride> overrides,
                            ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
        this.overrides = overrides;
        state = ctx.REVOKE() != null ? "REVOKE" : "GRANT";
    }

    @Override
    public void parseObject() {
        if (settings.isIgnorePrivileges() || isRefMode()) {
            addOutlineRefForCommentOrRule(state, ctx);
            return;
        }

        var usersOrRoles = getUsersOrRoles();
        boolean isGrantOption = ctx.with_option().stream().anyMatch(el -> el.GRANT() != null);

        for (var priv : ctx.privileges().privilege()) {
            String objectName = getFullCtxText(priv.names_references());

            //parsed columns
            Columns_permissionsContext columnsCtx = priv.columns_permissions();
            if (columnsCtx != null) {
                parseColumns(columnsCtx, objectName, usersOrRoles, isGrantOption);
                return;
            }

            List<String> permissions = priv.permissions().permission().stream()
                    .map(ParserAbstract::getFullCtxText)
                    .map(e -> e.toUpperCase(Locale.ROOT))
                    .toList();

            // 1 privilege for each user or role
            for (var user : usersOrRoles) {
                var userName = user.getText();
                AbstractStatement st = getStatement(userName);
                if (st == null) {
                    continue;
                }
                addObjReference(List.of(user), st.getStatementType(), state);
                // 1 privilege for each permission
                for (String per : permissions) {
                    addPrivilege(st, new ObjectPrivilege(state, per, objectName,
                            ChDiffUtils.getQuotedName(userName), isGrantOption, DatabaseType.CH));
                }
            }
        }
    }

    private void parseColumns(Columns_permissionsContext columnsCtx, String objectName,
                              List<IdentifierContext> usersOrRoles, boolean isGrantOption) {
        // collect information about column privileges
        Map<String, Entry<IdentifierContext, List<String>>> colPriv = new HashMap<>();
        for (var col : columnsCtx.table_column_privileges()) {
            String privName = getFullCtxText(col.permission()).toUpperCase(Locale.ROOT);
            for (var colName : col.identifier_list().identifier()) {
                colPriv.computeIfAbsent(colName.getText(), k -> new SimpleEntry<>(colName, new ArrayList<>()))
                        .getValue().add(privName);
            }
        }
        setColumnPrivilege(objectName, colPriv, usersOrRoles, isGrantOption);
    }

    private void setColumnPrivilege(String objectName, Map<String, Entry<IdentifierContext, List<String>>> colPrivs,
                                    List<IdentifierContext> usersOrRoles, boolean isGrantOption) {
        for (Entry<String, Entry<IdentifierContext, List<String>>> colPriv : colPrivs.entrySet()) {
            StringBuilder permission = new StringBuilder();
            for (String priv : colPriv.getValue().getValue()) {
                permission.append(priv).append('(').append(colPriv.getValue().getKey().getText()).append("),");
            }
            permission.setLength(permission.length() - 1);

            for (var user : usersOrRoles) {
                var userName = user.getText();
                var st = getStatement(userName);
                if (st == null) {
                    continue;
                }

                addObjReference(List.of(user), st.getStatementType(), state);
                addPrivilege(st, new ObjectPrivilege(state, permission.toString(), objectName,
                        userName, isGrantOption, DatabaseType.CH));
            }
        }
    }

    // get user or role statement
    private AbstractStatement getStatement(String name) {
        AbstractStatement st = db.getChild(name, DbObjType.USER);
        return st != null ? st : db.getChild(name, DbObjType.ROLE);
    }

    private List<IdentifierContext> getUsersOrRoles() {
        // for skipping CURRENT_USER
        List<IdentifierContext> usersOrRoles = new ArrayList<>();
        var ids = ctx.users().roles;
        for (var user : ids.identifier()) {
            if ("CURRENT_USER".equalsIgnoreCase(user.getText())) {
                continue;
            }
            usersOrRoles.add(user);
        }
        return usersOrRoles;
    }

    private void addPrivilege(AbstractStatement st, ObjectPrivilege privilege) {
        if (overrides == null) {
            st.addPrivilege(privilege);
        } else {
            overrides.computeIfAbsent(st, k -> new StatementOverride()).addPrivilege(privilege);
        }
    }

    @Override
    protected String getStmtAction() {
        return state;
    }
}