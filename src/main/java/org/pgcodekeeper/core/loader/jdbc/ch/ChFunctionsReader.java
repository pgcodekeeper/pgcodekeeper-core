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
package org.pgcodekeeper.core.loader.jdbc.ch;

import org.pgcodekeeper.core.loader.QueryBuilder;
import org.pgcodekeeper.core.loader.jdbc.AbstractStatementReader;
import org.pgcodekeeper.core.loader.jdbc.JdbcLoaderBase;
import org.pgcodekeeper.core.loader.jdbc.XmlReaderException;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.ch.statement.CreateChFunction;
import org.pgcodekeeper.core.schema.GenericColumn;
import org.pgcodekeeper.core.schema.ch.ChDatabase;
import org.pgcodekeeper.core.schema.ch.ChFunction;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Reader for ClickHouse functions.
 * Loads function definitions from system.functions table.
 */
public final class ChFunctionsReader extends AbstractStatementReader {

    private final ChDatabase db;

    /**
     * Creates a new ChFunctionsReader.
     *
     * @param loader the JDBC loader instance
     * @param db     the ClickHouse database to load functions into
     */
    public ChFunctionsReader(JdbcLoaderBase loader, ChDatabase db) {
        super(loader);
        this.db = db;
    }

    @Override
    protected void processResult(ResultSet result) throws SQLException, XmlReaderException {
        String name = result.getString("name");
        loader.setCurrentObject(new GenericColumn(name, DbObjType.FUNCTION));

        ChFunction function = new ChFunction(name);
        String definition = result.getString("create_query");

        loader.submitChAntlrTask(definition,
                p -> p.ch_file().query(0).stmt().ddl_stmt().create_stmt().create_function_stmt(),
                ctx -> new CreateChFunction(ctx, db, loader.getSettings()).parseObject(function));

        db.addChild(function);
    }

    @Override
    protected void fillQueryBuilder(QueryBuilder builder) {
        builder
                .column("create_query")
                .column("name")
                .from("system.functions")
                .where("origin != 'System'");
    }
}