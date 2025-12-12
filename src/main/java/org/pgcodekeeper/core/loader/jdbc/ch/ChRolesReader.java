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
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.api.schema.GenericColumn;
import org.pgcodekeeper.core.database.ch.schema.ChDatabase;
import org.pgcodekeeper.core.database.ch.schema.ChRole;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Reader for ClickHouse roles.
 * Loads role definitions from system.roles table.
 */
public final class ChRolesReader extends AbstractStatementReader {

    private final ChDatabase db;

    /**
     * Creates a new ChRolesReader.
     *
     * @param loader the JDBC loader instance
     * @param db     the ClickHouse database to load roles into
     */
    public ChRolesReader(JdbcLoaderBase loader, ChDatabase db) {
        super(loader);
        this.db = db;
    }

    @Override
    protected void processResult(ResultSet result) throws SQLException {
        String name = result.getString("name");
        loader.setCurrentObject(new GenericColumn(name, DbObjType.ROLE));

        ChRole role = new ChRole(name);
        String storage = result.getString("storage");
        if (storage != null) {
            role.setStorageType(storage);
        }
        db.addChild(role);
    }

    @Override
    protected void fillQueryBuilder(QueryBuilder builder) {
        builder
                .column("name")
                .column("storage")
                .from("system.roles");
    }
}