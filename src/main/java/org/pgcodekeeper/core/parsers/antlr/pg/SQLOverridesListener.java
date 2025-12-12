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
package org.pgcodekeeper.core.parsers.antlr.pg;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.database.api.schema.IRelation;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.database.base.schema.AbstractDatabase;
import org.pgcodekeeper.core.database.base.schema.AbstractSchema;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.database.base.schema.StatementOverride;
import org.pgcodekeeper.core.loader.ParserListenerMode;
import org.pgcodekeeper.core.parsers.antlr.base.CustomParserListener;
import org.pgcodekeeper.core.parsers.antlr.base.QNameParser;
import org.pgcodekeeper.core.exception.UnresolvedReferenceException;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.*;
import org.pgcodekeeper.core.parsers.antlr.pg.statement.AlterOwner;
import org.pgcodekeeper.core.parsers.antlr.pg.statement.GrantPrivilege;
import org.pgcodekeeper.core.parsers.antlr.pg.statement.PgParserAbstract;
import org.pgcodekeeper.core.database.pg.schema.PgDatabase;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.monitor.IMonitor;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Custom ANTLR listener for processing PostgreSQL SQL statements with override support.
 * Handles schema and ownership changes while applying statement overrides.
 */
public final class SQLOverridesListener extends CustomParserListener<PgDatabase>
        implements SqlContextProcessor {

    private final Map<AbstractStatement, StatementOverride> overrides;

    /**
     * Creates a new listener with override support.
     *
     * @param db        the target database schema
     * @param filename  name of the file being parsed
     * @param mode      parsing mode
     * @param errors    list to collect parsing errors
     * @param mon       progress monitor for cancellation support
     * @param overrides map of statement overrides to apply
     * @param settings  application settings
     */
    public SQLOverridesListener(PgDatabase db, String filename, ParserListenerMode mode, List<Object> errors,
                                IMonitor mon, Map<AbstractStatement, StatementOverride> overrides, ISettings settings) {
        super(db, filename, mode, errors, mon, settings);
        this.overrides = overrides;
    }

    /**
     * Processes the complete SQL file context.
     * Extracts and processes schema statements that may need overrides.
     *
     * @param rootCtx the root file context from ANTLR parser
     * @param stream  the token stream associated with the context
     */
    @Override
    public void process(SqlContext rootCtx, CommonTokenStream stream) {
        for (StatementContext s : rootCtx.statement()) {
            Schema_statementContext st = s.schema_statement();
            if (st != null) {
                Schema_createContext create = st.schema_create();
                Schema_alterContext alter;
                if (create != null) {
                    create(create);
                } else if ((alter = st.schema_alter()) != null) {
                    alter(alter);
                }
            }
        }
    }

    private void create(Schema_createContext ctx) {
        Rule_commonContext rule = ctx.rule_common();
        Create_schema_statementContext schema;
        if (rule != null) {
            safeParseStatement(new GrantPrivilege(rule, db, overrides, settings), ctx);
        } else if ((schema = ctx.create_schema_statement()) != null) {
            safeParseStatement(() -> createSchema(schema), ctx);
        }
    }

    private void alter(Schema_alterContext ctx) {
        Alter_owner_statementContext owner = ctx.alter_owner_statement();
        Alter_table_statementContext ats;
        if (owner != null) {
            safeParseStatement(new AlterOwner(owner, db, overrides, settings), ctx);
        } else if ((ats = ctx.alter_table_statement()) != null) {
            safeParseStatement(() -> alterTable(ats), ctx);
        }
    }

    private void createSchema(Create_schema_statementContext ctx) {
        User_nameContext user = ctx.user_name();
        IdentifierContext owner = user == null ? null : user.identifier();
        if (settings.isIgnorePrivileges() || owner == null) {
            return;
        }

        AbstractStatement st = getSafe(AbstractDatabase::getSchema, db, ctx.name);
        if (st.getName().equals(Consts.PUBLIC) && "postgres".equals(owner.getText())) {
            return;
        }

        overrides.computeIfAbsent(st, k -> new StatementOverride()).setOwner(owner.getText());
    }

    private void alterTable(Alter_table_statementContext ctx) {
        List<ParserRuleContext> ids = PgParserAbstract.getIdentifiers(ctx.name);
        ParserRuleContext schemaCtx = QNameParser.getSchemaNameCtx(ids);
        AbstractSchema schema = schemaCtx == null ? db.getDefaultSchema() :
                getSafe(AbstractDatabase::getSchema, db, schemaCtx);

        ParserRuleContext nameCtx = QNameParser.getFirstNameCtx(ids);

        for (Table_actionContext tablAction : ctx.table_action()) {
            Owner_toContext owner = tablAction.owner_to();
            IdentifierContext name;
            if (owner != null && (name = owner.user_name().identifier()) != null) {
                IRelation st = getSafe(AbstractSchema::getRelation, schema, nameCtx);
                overrides.computeIfAbsent((AbstractStatement) st,
                        k -> new StatementOverride()).setOwner(name.getText());
            }
        }
    }

    private <T extends IStatement, R extends IStatement> R getSafe(
            BiFunction<T, String, R> getter, T container, ParserRuleContext ctx) {
        String name = ctx.getText();
        R statement = getter.apply(container, name);
        if (statement == null) {
            throw new UnresolvedReferenceException("Cannot find object in database: "
                    + name, ctx.getStart());
        }

        return statement;
    }
}
