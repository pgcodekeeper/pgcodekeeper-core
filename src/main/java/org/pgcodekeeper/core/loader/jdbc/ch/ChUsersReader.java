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

import org.pgcodekeeper.core.ChDiffUtils;
import org.pgcodekeeper.core.loader.QueryBuilder;
import org.pgcodekeeper.core.loader.jdbc.AbstractStatementReader;
import org.pgcodekeeper.core.loader.jdbc.JdbcLoaderBase;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.api.schema.GenericColumn;
import org.pgcodekeeper.core.database.ch.schema.ChDatabase;
import org.pgcodekeeper.core.database.ch.schema.ChUser;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Reader for ClickHouse users.
 * Loads user definitions from system.users table.
 */
public final class ChUsersReader extends AbstractStatementReader {

    private static final String LOCAL = "localhost";
    private static final String IP = "::/0";
    private final ChDatabase db;

    /**
     * Creates a new ChUsersReader.
     *
     * @param loader the JDBC loader instance
     * @param db     the ClickHouse database to load users into
     */
    public ChUsersReader(JdbcLoaderBase loader, ChDatabase db) {
        super(loader);
        this.db = db;
    }

    @Override
    protected void processResult(ResultSet res) throws SQLException {
        String name = res.getString("name");
        loader.setCurrentObject(new GenericColumn(name, DbObjType.USER));

        ChUser user = new ChUser(name);
        String storage = res.getString("storage");
        if (storage != null) {
            user.setStorageType(storage);
        }

        fillHosts(res, user);

        ChJdbcUtils.addRoles(res, "default_roles_list", "default_roles_except", user,
                ChUser::addDefRole, ChUser::addExceptRole);

        ChJdbcUtils.addRoles(res, "grantees_list", "grantees_except", user,
                ChUser::addGrantee, ChUser::addExGrantee);

        var defDb = res.getString("default_database");
        if (!defDb.isBlank()) {
            user.setDefaultDatabase(defDb);
        }

        db.addChild(user);
    }

    private void fillHosts(ResultSet res, ChUser user) throws SQLException {
        boolean isAnyHost = false;

        String[] hostNames = ChJdbcUtils.getColArray(res, "host_names");
        if (hostNames != null) {
            for (String hostName : hostNames) {
                user.addHost(LOCAL.equals(hostName) ? "LOCAL" : "NAME " + ChDiffUtils.quoteLiteralName(hostName));
            }
        }

        String[] hostIps = ChJdbcUtils.getColArray(res, "host_ip");
        if (hostIps != null) {
            for (String ip : hostIps) {
                if (!IP.equals(ip)) {
                    user.addHost("IP " + ChDiffUtils.quoteLiteralName(ip));
                } else {
                    isAnyHost = true;
                }
            }
        }

        String[] hostRegexps = ChJdbcUtils.getColArray(res, "host_names_regexp");
        if (hostRegexps != null) {
            for (String regexp : hostRegexps) {
                user.addHost("REGEXP " + ChDiffUtils.quoteLiteralName(regexp));
            }
        }

        String[] hostLike = ChJdbcUtils.getColArray(res, "host_names_like");
        if (hostLike != null) {
            for (String like : hostLike) {
                user.addHost("LIKE " + ChDiffUtils.quoteLiteralName(like));
            }
        }

        if (!isAnyHost && user.hasHosts()) {
            user.addHost("NONE");
        }
    }

    @Override
    protected void fillQueryBuilder(QueryBuilder builder) {
        builder
                .column("name")
                .column("storage")
                .column("host_ip")
                .column("host_names")
                .column("host_names_regexp")
                .column("host_names_like")
                .column("default_roles_list")
                .column("default_roles_except")
                .column("default_database")
                .column("grantees_list")
                .column("grantees_except")
                .from("system.users");
    }
}
