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
package org.pgcodekeeper.core.parsers.antlr.ms;

import org.antlr.v4.runtime.CommonTokenStream;
import org.pgcodekeeper.core.loader.ParserListenerMode;
import org.pgcodekeeper.core.parsers.antlr.base.CustomParserListener;
import org.pgcodekeeper.core.exception.UnresolvedReferenceException;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.*;
import org.pgcodekeeper.core.parsers.antlr.ms.statement.AlterMsAuthorization;
import org.pgcodekeeper.core.parsers.antlr.ms.statement.GrantMsPrivilege;
import org.pgcodekeeper.core.schema.PgStatement;
import org.pgcodekeeper.core.schema.StatementOverride;
import org.pgcodekeeper.core.schema.ms.MsDatabase;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.monitor.IMonitor;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Custom ANTLR listener for processing Microsoft SQL Server (T-SQL) statements with override support.
 * Handles authorization and privilege statements while applying statement overrides.
 */
public final class TSQLOverridesListener extends CustomParserListener<MsDatabase>
        implements TSqlContextProcessor {

    private final Map<PgStatement, StatementOverride> overrides;

    /**
     * Creates a new T-SQL listener with override support.
     *
     * @param db        the target Microsoft SQL Server database schema
     * @param filename  name of the file being parsed
     * @param mode      parsing mode (NORMAL or SCRIPT)
     * @param errors    list to collect parsing errors
     * @param mon       progress monitor for cancellation support
     * @param overrides map of statement overrides to apply
     * @param settings  application settings
     */
    public TSQLOverridesListener(MsDatabase db, String filename, ParserListenerMode mode, List<Object> errors,
                                 IMonitor mon, Map<PgStatement, StatementOverride> overrides, ISettings settings) {
        super(db, filename, mode, errors, mon, settings);
        this.overrides = overrides;
    }

    /**
     * Processes the complete T-SQL file context.
     * Extracts and processes batches and statements that may need overrides.
     *
     * @param rootCtx the root file context from ANTLR parser
     * @param stream  the token stream associated with the context
     */
    @Override
    public void process(Tsql_fileContext rootCtx, CommonTokenStream stream) {
        for (BatchContext b : rootCtx.batch()) {
            Sql_clausesContext clauses = b.sql_clauses();
            Batch_statementContext batch;
            if (clauses != null) {
                for (St_clauseContext st : clauses.st_clause()) {
                    clause(st);
                }
            } else if ((batch = b.batch_statement()) != null) {
                safeParseStatement(() -> batch(batch), batch);
            }
        }
    }

    private void clause(St_clauseContext st) {
        Ddl_clauseContext ddl = st.ddl_clause();
        Another_statementContext ast;
        if (ddl != null && !settings.isIgnorePrivileges()) {
            Schema_createContext create = ddl.schema_create();
            Schema_alterContext alter;
            if (create != null) {
                safeParseStatement(() -> create(create), create);
            } else if ((alter = ddl.schema_alter()) != null) {
                alter(alter);
            }
        } else if ((ast = st.another_statement()) != null) {
            Security_statementContext ss;
            if ((ss = ast.security_statement()) != null && ss.rule_common() != null) {
                safeParseStatement(new GrantMsPrivilege(ss.rule_common(), db, overrides, settings), ss);
            }
        }
    }

    private void create(Schema_createContext ctx) {
        Create_assemblyContext ass = ctx.create_assembly();
        if (ass != null && ass.owner_name != null) {
            computeOverride(MsDatabase::getAssembly, ass.assembly_name, ass.owner_name);
        }
    }

    private void alter(Schema_alterContext ctx) {
        if (ctx.alter_authorization() != null) {
            safeParseStatement(new AlterMsAuthorization(
                    ctx.alter_authorization(), db, overrides, settings), ctx);
        }
    }

    private void batch(Batch_statementContext batch) {
        Create_schemaContext schema = batch.create_schema();
        if (schema != null && schema.owner_name != null) {
            computeOverride(MsDatabase::getSchema, schema.schema_name, schema.owner_name);
        }
    }

    private <R extends PgStatement> void computeOverride(
            BiFunction<MsDatabase, String, R> getter, IdContext nameCtx, IdContext ownerCtx) {
        String name = nameCtx.getText();
        R statement = getter.apply(db, name);
        if (statement == null) {
            throw new UnresolvedReferenceException("Cannot find object in database: "
                    + name, nameCtx.getStart());
        }

        String owner = ownerCtx.getText();
        overrides.computeIfAbsent(statement, k -> new StatementOverride()).setOwner(owner);
    }
}
