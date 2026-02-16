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
package org.pgcodekeeper.core.database.pg.parser.statement;

import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.parser.QNameParser;
import org.pgcodekeeper.core.database.base.parser.statement.ParserAbstract;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.database.pg.parser.generated.SQLParser.*;
import org.pgcodekeeper.core.database.pg.schema.*;
import org.pgcodekeeper.core.database.pg.utils.PgDiffUtils;
import org.pgcodekeeper.core.settings.ISettings;

/**
 * Parser for PostgreSQL GRANT and REVOKE privilege statements.
 * <p>
 * This class handles parsing of privilege management statements including
 * object privileges (tables, sequences, functions), column privileges,
 * schema privileges, and various privilege types like SELECT, INSERT,
 * UPDATE, DELETE, USAGE, etc.
 */
public final class PgGrantPrivilege extends PgParserAbstract {
    private final Rule_commonContext ctx;
    private final String state;
    private final boolean isGO;
    private final Map<AbstractStatement, StatementOverride> overrides;

    /**
     * Constructs a new GrantPrivilege parser without statement overrides.
     *
     * @param ctx      the rule common context (GRANT/REVOKE statement)
     * @param db       the PostgreSQL database object
     * @param settings the ISettings object
     */
    public PgGrantPrivilege(Rule_commonContext ctx, PgDatabase db, ISettings settings) {
        this(ctx, db, null, settings);
    }

    /**
     * Constructs a new GrantPrivilege parser with optional statement overrides.
     *
     * @param ctx       the rule common context (GRANT/REVOKE statement)
     * @param db        the PostgreSQL database object
     * @param overrides optional map for statement overrides, may be null
     * @param settings  ISettings object
     */
    public PgGrantPrivilege(Rule_commonContext ctx, PgDatabase db, Map<AbstractStatement, StatementOverride> overrides,
                          ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
        this.overrides = overrides;
        state = ctx.REVOKE() != null ? "REVOKE" : "GRANT";
        isGO = ctx.OPTION() != null;
    }

    @Override
    public void parseObject() {
        Rule_member_objectContext obj = ctx.rule_member_object();
        // unsupported roles rules, ALL TABLES/SEQUENCES/FUNCTIONS IN SCHEMA
        if (settings.isIgnorePrivileges() || ctx.other_rules() != null || obj.ALL() != null) {
            addOutlineRefForCommentOrRule(state, ctx);
            return;
        }

        List<String> roles = parseRoles();
        if (roles.isEmpty()) {
            return;
        }

        Columns_permissionsContext columnsCtx = ctx.columns_permissions();

        if (columnsCtx != null) {
            parseColumns(columnsCtx, roles);
            return;
        }

        String permissions = ctx.permissions().permission().stream()
                .map(ParserAbstract::getFullCtxText)
                .map(e -> e.toUpperCase(Locale.ROOT))
                .collect(Collectors.joining(","));

        if (obj.FUNCTION() != null || obj.PROCEDURE() != null) {
            parseFunction(obj, permissions, roles);
            return;
        }

        DbObjType type = null;
        if (obj.table_names != null) {
            type = DbObjType.TABLE;
        } else if (obj.SEQUENCE() != null) {
            type = DbObjType.SEQUENCE;
        } else if (obj.DOMAIN() != null) {
            type = DbObjType.DOMAIN;
        } else if (obj.SCHEMA() != null) {
            type = DbObjType.SCHEMA;
        } else if (obj.TYPE() != null) {
            type = DbObjType.TYPE;
        } else if (obj.FOREIGN() != null) {
            if (obj.SERVER() != null) {
                type = DbObjType.SERVER;
            } else if (obj.WRAPPER() != null) {
                type = DbObjType.FOREIGN_DATA_WRAPPER;
            }
        }

        if (type == null) {
            addOutlineRefForCommentOrRule(state, ctx);
            return;
        }

        for (Schema_qualified_nameContext name : obj.names_references().schema_qualified_name()) {
            addObjReference(getIdentifiers(name), type, state);

            if (!isRefMode()) {
                addToDB(name, type, state, permissions, roles, isGO);
            }
        }
    }

    private List<String> parseRoles() {
        List<String> roles = new ArrayList<>();
        for (Role_name_with_groupContext roleCtx : ctx.roles_names().role_name_with_group()) {
            // skip CURRENT_USER and SESSION_USER
            IdentifierContext user = roleCtx.user_name().identifier();
            if (user == null) {
                continue;
            }
            String role = getFullCtxText(user);
            if (roleCtx.GROUP() != null) {
                role = "GROUP " + role;
            }

            roles.add(role);
        }

        return roles;
    }

    private void parseFunction(Rule_member_objectContext obj, String permissions, List<String> roles) {
        for (Function_parametersContext funct : obj.func_name) {
            List<ParserRuleContext> funcIds = getIdentifiers(funct.schema_qualified_name());
            ParserRuleContext functNameCtx = QNameParser.getFirstNameCtx(funcIds);
            PgSchema schema = getSchemaSafe(funcIds);
            PgAbstractFunction func = getSafe(PgSchema::getFunction, schema,
                    parseSignature(functNameCtx.getText(), funct.function_args()),
                    functNameCtx.getStart());

            StringBuilder sb = new StringBuilder();
            DbObjType type = obj.PROCEDURE() == null ? DbObjType.FUNCTION : DbObjType.PROCEDURE;
            addObjReference(funcIds, type, state, parseArguments(funct.function_args()));

            if (isRefMode()) {
                continue;
            }

            sb.append(type).append(' ');
            sb.append(PgDiffUtils.getQuotedName(schema.getName())).append('.');

            // For AGGREGATEs in GRANT/REVOKE the signature will be the same as in FUNCTIONs;
            // important: asterisk (*) and 'ORDER BY' are not displayed.
            func.appendFunctionSignature(sb, false, true);

            for (String role : roles) {
                addPrivilege(func, new PgPrivilege(state, permissions, sb.toString(), role, isGO));
            }
        }
    }

    private void parseColumns(Columns_permissionsContext columnsCtx, List<String> roles) {
        // collect information about column privileges
        Map<String, Entry<IdentifierContext, List<String>>> colPriv = new HashMap<>();
        for (Table_column_privilegesContext priv : columnsCtx.table_column_privileges()) {
            String privName = getFullCtxText(priv.table_column_privilege()).toUpperCase(Locale.ROOT);
            for (IdentifierContext col : priv.identifier_list_in_paren().identifier_list().identifier()) {
                colPriv.computeIfAbsent(col.getText(),
                        k -> new SimpleEntry<>(col, new ArrayList<>())).getValue().add(privName);
            }
        }

        // parse objects
        for (Schema_qualified_nameContext tbl : ctx.rule_member_object().names_references().schema_qualified_name()) {
            setColumnPrivilege(tbl, colPriv, roles);
        }
    }

    private void setColumnPrivilege(Schema_qualified_nameContext tbl,
                                    Map<String, Entry<IdentifierContext, List<String>>> colPrivs, List<String> roles) {
        List<ParserRuleContext> ids = getIdentifiers(tbl);

        addObjReference(ids, DbObjType.TABLE, state);

        // TODO waits for column references
        // addObjReference(Arrays.asList(QNameParser.getSchemaNameCtx(ids), firstPart, colName), DbObjType.COLUMN, StatementActions.NONE)

        if (isRefMode()) {
            return;
        }

        PgSchema schema = getSchemaSafe(ids);
        ParserRuleContext firstPart = QNameParser.getFirstNameCtx(ids);

        // write privileges as we received them in one line
        AbstractStatement st = (AbstractStatement) getSafe(PgSchema::getRelation, schema, firstPart);

        for (Entry<String, Entry<IdentifierContext, List<String>>> colPriv : colPrivs.entrySet()) {
            StringBuilder permission = new StringBuilder();
            for (String priv : colPriv.getValue().getValue()) {
                permission.append(priv).append('(')
                        .append(colPriv.getValue().getKey().getText()).append("),");
            }

            permission.setLength(permission.length() - 1);

            for (String role : roles) {
                IPrivilege priv = new PgPrivilege(state, permission.toString(),
                        "TABLE " + st.getQualifiedName(), role, isGO);
                if (DbObjType.TABLE != st.getStatementType()) {
                    addPrivilege(st, priv);
                } else {
                    IdentifierContext colName = colPriv.getValue().getKey();
                    PgColumn col = getSafe(PgAbstractTable::getColumn, (PgAbstractTable) st, colName);
                    addPrivilege(col, priv);
                }
            }
        }
    }

    private void addToDB(Schema_qualified_nameContext name, DbObjType type,
                         String state, String permissions, List<String> roles, boolean isGO) {
        List<ParserRuleContext> ids = getIdentifiers(name);
        ParserRuleContext idCtx = QNameParser.getFirstNameCtx(ids);
        AbstractStatement statement = switch (type) {
            case SCHEMA -> getSafe(PgDatabase::getSchema, db, idCtx);
            case DOMAIN -> getSafe(PgSchema::getDomain, getSchemaSafe(ids), idCtx);
            case TABLE -> (AbstractStatement) getSafe(PgSchema::getRelation, getSchemaSafe(ids), idCtx);
            case SEQUENCE -> getSafe(PgSchema::getSequence, getSchemaSafe(ids), idCtx);
            case FOREIGN_DATA_WRAPPER -> getSafe(PgDatabase::getForeignDW, db, idCtx);
            case SERVER -> getSafe(PgDatabase::getServer, db, idCtx);
            case TYPE -> {
                PgSchema schema = getSchemaSafe(ids);
                statement = schema.getType(idCtx.getText());

                // if type not found try domain
                if (statement == null) {
                    statement = getSafe(PgSchema::getDomain, schema, idCtx);
                }
                yield statement;
            }
            default -> null;
        };
        if (statement != null) {
            String typeName = type == DbObjType.SERVER ? "FOREIGN SERVER" : type.getTypeName();
            for (String role : roles) {
                addPrivilege(statement, new PgPrivilege(state, permissions,
                        typeName + " " + statement.getQualifiedName(), role, isGO));
            }
        }
    }

    private void addPrivilege(AbstractStatement st, IPrivilege privilege) {
        if (overrides == null) {
            doSafe(AbstractStatement::addPrivilege, st, privilege);
        } else {
            overrides.computeIfAbsent(st,
                    k -> new StatementOverride()).addPrivilege(privilege);
        }
    }

    @Override
    protected String getStmtAction() {
        return state;
    }
}