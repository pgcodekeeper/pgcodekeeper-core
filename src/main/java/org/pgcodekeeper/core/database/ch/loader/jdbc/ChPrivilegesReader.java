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
package org.pgcodekeeper.core.database.ch.loader.jdbc;

import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.database.ch.ChDiffUtils;
import org.pgcodekeeper.core.database.ch.schema.ChDatabase;
import org.pgcodekeeper.core.database.ch.schema.ChPrivilege;
import org.pgcodekeeper.core.loader.QueryBuilder;
import org.pgcodekeeper.core.loader.jdbc.AbstractStatementReader;
import org.pgcodekeeper.core.loader.jdbc.JdbcLoaderBase;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Reader for ClickHouse privileges.
 * Loads privilege definitions from system.grants table.
 */
public final class ChPrivilegesReader extends AbstractStatementReader {

    private final ChDatabase db;

    /**
     * Creates a new ChPrivilegesReader.
     *
     * @param loader the JDBC loader instance
     * @param db     the ClickHouse database to load privileges into
     */
    public ChPrivilegesReader(JdbcLoaderBase loader, ChDatabase db) {
        super(loader);
        this.db = db;
    }

    @Override
    protected void processResult(ResultSet result) throws SQLException {
        String user = result.getString("user_name");
        String role = result.getString("role_name");
        AbstractStatement st = user != null ? db.getChild(user, DbObjType.USER) : db.getChild(role, DbObjType.ROLE);

        String database = getNameOrAsterisk(result.getString("database"));
        String table = getNameOrAsterisk(result.getString("table"));
        String col = result.getString("column");
        String fullName = database + '.' + table;

        String permission = result.getString("access_type");
        String columnStr = col == null ? "" : '(' + col + ')';
        boolean isGrantOption = result.getBoolean("grant_option");

        st.addPrivilege(new ChPrivilege("GRANT", permission + columnStr, fullName,
                user != null ? user : role, isGrantOption));
    }

    private String getNameOrAsterisk(String name) {
        if (name == null) {
            return "*";
        }

        return ChDiffUtils.getQuotedName(name);
    }

    @Override
    protected void fillQueryBuilder(QueryBuilder builder) {
        builder
                .column("role_name")
                .column("user_name")
                .column("table")
                .column("access_type")
                .column("database")
                .column("column")
                .column("grant_option")
                .from("system.grants")
                .where("user_name != 'default'");
    }
}