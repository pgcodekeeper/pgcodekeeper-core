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
package org.pgcodekeeper.core.loader.jdbc.pg;

import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.loader.QueryBuilder;
import org.pgcodekeeper.core.loader.jdbc.AbstractStatementReader;
import org.pgcodekeeper.core.loader.jdbc.JdbcLoaderBase;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.schema.AbstractSchema;
import org.pgcodekeeper.core.schema.GenericColumn;
import org.pgcodekeeper.core.schema.pg.PgDatabase;
import org.pgcodekeeper.core.schema.pg.PgSchema;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Reader for PostgreSQL schemas.
 * Loads schema definitions from pg_namespace system catalog.
 */
public class SchemasReader extends AbstractStatementReader {

    private final PgDatabase db;

    /**
     * Creates a new SchemasReader.
     *
     * @param loader the JDBC loader instance
     * @param db     the PostgreSQL database instance
     */
    public SchemasReader(JdbcLoaderBase loader, PgDatabase db) {
        super(loader);
        this.db = db;
    }

    @Override
    protected void processResult(ResultSet res) throws SQLException {
        String schemaName = res.getString("nspname");
        loader.setCurrentObject(new GenericColumn(schemaName, DbObjType.SCHEMA));
        if (loader.isIgnoredSchema(schemaName)) {
            return;
        }

        AbstractSchema s = new PgSchema(schemaName);
        long owner = res.getLong("nspowner");

        if (!schemaName.equals(Consts.PUBLIC)) {
            loader.setOwner(s, owner);
            loader.setComment(s, res);
        } else if (!"postgres".equals(loader.getRoleByOid(owner))) {
            loader.setOwner(s, owner);
        }

        loader.setPrivileges(s, res.getString("nspacl"), null);
        loader.setAuthor(s, res);
        loader.putSchema(res.getLong("oid"), s);

        db.addSchema(s);
    }

    @Override
    protected String getClassId() {
        return "pg_namespace";
    }

    @Override
    protected void fillQueryBuilder(QueryBuilder builder) {
        addExtensionDepsCte(builder);
        addDescriptionPart(builder);

        builder
                .column("res.oid")
                .column("res.nspname")
                .column("res.nspacl")
                .column("res.nspowner")
                .from("pg_catalog.pg_namespace res")
                .where("res.nspname NOT LIKE 'pg\\_%'")
                .where("res.nspname != 'information_schema'");
    }
}
