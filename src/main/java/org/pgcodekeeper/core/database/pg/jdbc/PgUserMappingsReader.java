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
package org.pgcodekeeper.core.database.pg.jdbc;

import java.sql.*;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.jdbc.QueryBuilder;
import org.pgcodekeeper.core.database.base.parser.statement.ParserAbstract;
import org.pgcodekeeper.core.database.pg.loader.PgJdbcLoader;
import org.pgcodekeeper.core.database.pg.schema.*;

/**
 * Reader for PostgreSQL user mappings.
 * Loads user mapping definitions from pg_user_mapping system catalog.
 */
public final class PgUserMappingsReader extends PgAbstractJdbcReader {

    private final PgDatabase db;

    /**
     * Constructs a new PgUserMappingsReader.
     *
     * @param loader the JDBC loader base instance
     * @param db     the PostgreSQL database instance
     */
    public PgUserMappingsReader(PgJdbcLoader loader, PgDatabase db) {
        super(loader);
        this.db = db;
    }

    @Override
    protected void processResult(ResultSet res) throws SQLException {
        String user = res.getString("username");
        String server = res.getString("servername");
        if (user == null) {
            // https://www.postgresql.org/docs/current/catalog-pg-user-mapping.html
            // zero if the user mapping is public
            user = "public";
        }

        PgUserMapping usm = new PgUserMapping(user, server);

        loader.setCurrentObject(new GenericColumn(usm.getName(), DbObjType.USER_MAPPING));
        usm.addDependency(new GenericColumn(usm.getServer(), DbObjType.SERVER));

        String[] options = PgJdbcUtils.getColArray(res, "umoptions", true);
        if (options != null) {
            ParserAbstract.fillOptionParams(options, usm::addOption, false, true, false);
        }

        loader.setAuthor(usm, res);
        db.addChild(usm);
    }

    @Override
    public String getClassId() {
        return "pg_user_mapping";
    }

    @Override
    protected void fillQueryBuilder(QueryBuilder builder) {
        addExtensionDepsCte(builder);

        builder
                .column("rol.rolname AS username")
                .column("fsrv.srvname AS servername")
                .column("res.umoptions")
                .from("pg_catalog.pg_user_mapping res")
                .join("LEFT JOIN pg_catalog.pg_foreign_server fsrv ON res.umserver = fsrv.oid")
                .join("LEFT JOIN pg_catalog.pg_roles rol ON res.umuser = rol.oid");
    }
}