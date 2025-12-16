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

import org.pgcodekeeper.core.PgDiffUtils;
import org.pgcodekeeper.core.loader.QueryBuilder;
import org.pgcodekeeper.core.loader.jdbc.AbstractStatementReader;
import org.pgcodekeeper.core.loader.jdbc.JdbcLoaderBase;
import org.pgcodekeeper.core.loader.jdbc.JdbcReader;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.base.statement.ParserAbstract;
import org.pgcodekeeper.core.database.api.schema.GenericColumn;
import org.pgcodekeeper.core.database.pg.schema.PgDatabase;
import org.pgcodekeeper.core.database.pg.schema.PgServer;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Reader for PostgreSQL foreign servers.
 * Loads foreign server definitions from pg_foreign_server system catalog.
 */
public final class PgServersReader extends AbstractStatementReader {

    private final PgDatabase db;

    /**
     * Creates a new PgServersReader.
     *
     * @param loader the JDBC loader base for database operations
     * @param db     the PostgreSQL database to add servers to
     */
    public PgServersReader(JdbcLoaderBase loader, PgDatabase db) {
        super(loader);
        this.db = db;
    }

    @Override
    protected void processResult(ResultSet res) throws SQLException {
        String srvName = res.getString("srvname");
        loader.setCurrentObject(new GenericColumn(srvName, DbObjType.SERVER));
        PgServer srv = new PgServer(srvName);
        srv.setFdw(res.getString("fdwname"));
        srv.addDependency(new GenericColumn(srv.getFdw(), DbObjType.FOREIGN_DATA_WRAPPER));
        String srvType = res.getString("srvtype");
        if (srvType != null) {
            srv.setType(PgDiffUtils.quoteString(srvType));
        }

        String srvVersion = res.getString("srvversion");
        if (srvVersion != null) {
            srv.setVersion(PgDiffUtils.quoteString(srvVersion));
        }

        String[] options = JdbcReader.getColArray(res, "srvoptions", true);
        if (options != null) {
            ParserAbstract.fillOptionParams(options, srv::addOption, false, true, false);
        }
        loader.setComment(srv, res);
        loader.setOwner(srv, res.getLong("srvowner"));
        loader.setPrivileges(srv, res.getString("srvacl"), null);
        loader.setAuthor(srv, res);
        db.addChild(srv);
    }

    @Override
    protected String getClassId() {
        return "pg_foreign_server";
    }

    @Override
    protected void fillQueryBuilder(QueryBuilder builder) {
        addExtensionDepsCte(builder);
        addDescriptionPart(builder);

        builder
                .column("res.srvname")
                .column("res.srvtype")
                .column("res.srvversion")
                .column("res.srvacl")
                .column("res.srvoptions")
                .column("res.srvowner")
                .column("fdw.fdwname")
                .from("pg_catalog.pg_foreign_server res")
                .join("LEFT JOIN pg_catalog.pg_foreign_data_wrapper fdw ON res.srvfdw = fdw.oid");
    }
}
