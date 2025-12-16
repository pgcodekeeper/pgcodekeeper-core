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
package org.pgcodekeeper.core.database.pg.loader.jdbc;

import org.pgcodekeeper.core.database.api.schema.EventType;
import org.pgcodekeeper.core.database.api.schema.GenericColumn;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.loader.QueryBuilder;
import org.pgcodekeeper.core.loader.jdbc.JdbcLoaderBase;
import org.pgcodekeeper.core.loader.jdbc.JdbcReader;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.pg.launcher.VexAnalysisLauncher;
import org.pgcodekeeper.core.database.pg.schema.PgPolicy;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Reader for PostgreSQL policies.
 * Loads policy definitions from pg_policy and related system catalogs.
 */
public class PgPoliciesReader extends JdbcReader {

    /**
     * Creates a new PgPoliciesReader.
     *
     * @param loader the JDBC loader instance
     */
    public PgPoliciesReader(JdbcLoaderBase loader) {
        super(loader);
    }

    @Override
    protected void processResult(ResultSet res, AbstractSchema schema) throws SQLException {
        String tableName = res.getString("relname");
        var c = schema.getStatementContainer(tableName);
        if (c == null) {
            return;
        }

        String schemaName = schema.getName();
        String policyName = res.getString("polname");
        loader.setCurrentObject(new GenericColumn(schemaName, tableName, policyName, DbObjType.POLICY));

        PgPolicy p = new PgPolicy(policyName);

        switch (res.getString("polcmd")) {
            case "r":
                p.setEvent(EventType.SELECT);
                break;
            case "w":
                p.setEvent(EventType.UPDATE);
                break;
            case "a":
                p.setEvent(EventType.INSERT);
                break;
            case "d":
                p.setEvent(EventType.DELETE);
                break;
        }

        String[] roles = getColArray(res, "polroles", true);
        if (roles != null) {
            for (String role : roles) {
                p.addRole(role);
            }
        }

        if (SupportedPgVersion.GP_VERSION_7.isLE(loader.getVersion())) {
            p.setPermissive(res.getBoolean("polpermissive"));
        }
        AbstractDatabase db = schema.getDatabase();

        String using = res.getString("polqual");
        if (using != null) {
            p.setUsing('(' + using + ')');
            loader.submitAntlrTask(using, parser -> parser.vex_eof().vex().get(0),
                    ctx -> db.addAnalysisLauncher(new VexAnalysisLauncher(p, ctx, loader.getCurrentLocation())));
        }

        String check = res.getString("polwithcheck");
        if (check != null) {
            p.setCheck('(' + check + ')');
            loader.submitAntlrTask(check, parser -> parser.vex_eof().vex().get(0),
                    ctx -> db.addAnalysisLauncher(new VexAnalysisLauncher(p, ctx, loader.getCurrentLocation())));
        }

        loader.setAuthor(p, res);
        loader.setComment(p, res);

        c.addPolicy(p);
    }

    @Override
    protected String getClassId() {
        return "pg_policy";
    }

    @Override
    protected String getSchemaColumn() {
        return "c.relnamespace";
    }

    @Override
    protected void fillQueryBuilder(QueryBuilder builder) {
        addExtensionSchemasCte(builder);
        addDescriptionPart(builder);

        builder
                .column("res.polname")
                .column("c.relname")
                .column("res.polcmd")
                .column("ARRAY(SELECT pg_catalog.quote_ident(rolname) FROM pg_catalog.pg_roles WHERE oid = ANY(res.polroles)) AS polroles")
                .column("pg_catalog.pg_get_expr(res.polqual, res.polrelid) AS polqual")
                .column("pg_catalog.pg_get_expr(res.polwithcheck, res.polrelid) AS polwithcheck")
                .from("pg_catalog.pg_policy res")
                .join("JOIN pg_catalog.pg_class c ON c.oid = res.polrelid");

        if (SupportedPgVersion.GP_VERSION_7.isLE(loader.getVersion())) {
            builder.column("res.polpermissive");
        }
    }
}
