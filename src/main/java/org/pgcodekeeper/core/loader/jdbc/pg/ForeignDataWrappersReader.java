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

import org.pgcodekeeper.core.loader.QueryBuilder;
import org.pgcodekeeper.core.loader.jdbc.AbstractStatementReader;
import org.pgcodekeeper.core.loader.jdbc.JdbcLoaderBase;
import org.pgcodekeeper.core.loader.jdbc.JdbcReader;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.base.statement.ParserAbstract;
import org.pgcodekeeper.core.parsers.antlr.pg.statement.CreateFdw;
import org.pgcodekeeper.core.schema.GenericColumn;
import org.pgcodekeeper.core.schema.pg.PgDatabase;
import org.pgcodekeeper.core.schema.pg.PgForeignDataWrapper;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Reader for PostgreSQL foreign data wrappers.
 * Loads foreign data wrapper definitions from pg_foreign_data_wrapper system catalog.
 */
public final class ForeignDataWrappersReader extends AbstractStatementReader {

    private final PgDatabase db;

    /**
     * Creates a new foreign data wrappers reader.
     *
     * @param loader the JDBC loader base for database operations
     * @param db     the PostgreSQL database to load foreign data wrappers into
     */
    public ForeignDataWrappersReader(JdbcLoaderBase loader, PgDatabase db) {
        super(loader);
        this.db = db;
    }

    @Override
    protected void processResult(ResultSet res) throws SQLException {
        String fdwName = res.getString("fdwname");
        loader.setCurrentObject(new GenericColumn(fdwName, DbObjType.FOREIGN_DATA_WRAPPER));
        PgForeignDataWrapper f = new PgForeignDataWrapper(fdwName);

        String fdwHandler = res.getString("fdwhandler");
        if (!"-".equals(fdwHandler)) {
            JdbcReader.setFunctionWithDep(PgForeignDataWrapper::setHandler, f, fdwHandler, CreateFdw.HANDLER_SIGNATURE);
        }

        String fdwValidator = res.getString("fdwvalidator");
        if (!"-".equals(fdwValidator)) {
            JdbcReader.setFunctionWithDep(PgForeignDataWrapper::setValidator, f, fdwValidator, CreateFdw.VALIDATOR_SIGNATURE);
        }

        String[] options = JdbcReader.getColArray(res, "fdwoptions");
        if (options != null) {
            ParserAbstract.fillOptionParams(options, f::addOption, false, true, false);
        }
        loader.setComment(f, res);
        loader.setOwner(f, res.getLong("fdwowner"));
        loader.setPrivileges(f, res.getString("fdwacl"), null);
        loader.setAuthor(f, res);
        db.addChild(f);
    }

    @Override
    protected String getClassId() {
        return "pg_foreign_data_wrapper";
    }

    @Override
    protected void fillQueryBuilder(QueryBuilder builder) {
        addExtensionDepsCte(builder);
        addDescriptionPart(builder);

        builder
                .column("res.fdwname")
                .column("res.fdwhandler::pg_catalog.regproc")
                .column("res.fdwvalidator::pg_catalog.regproc")
                .column("res.fdwoptions")
                .column("res.fdwacl")
                .column("res.fdwowner")
                .from("pg_catalog.pg_foreign_data_wrapper res");
    }
}
