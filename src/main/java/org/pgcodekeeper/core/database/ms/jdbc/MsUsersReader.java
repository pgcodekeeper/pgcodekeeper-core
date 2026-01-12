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
package org.pgcodekeeper.core.database.ms.jdbc;

import org.pgcodekeeper.core.database.base.jdbc.AbstractJdbcReader;
import org.pgcodekeeper.core.database.ms.loader.MsJdbcLoader;
import org.pgcodekeeper.core.database.base.jdbc.QueryBuilder;
import org.pgcodekeeper.core.exception.XmlReaderException;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.api.schema.GenericColumn;
import org.pgcodekeeper.core.database.ms.schema.MsDatabase;
import org.pgcodekeeper.core.database.ms.schema.MsUser;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Reader for Microsoft SQL users.
 * Loads user definitions from sys.database_principals system view.
 */
public class MsUsersReader extends AbstractJdbcReader<MsJdbcLoader> implements IMsJdbcReader {

    private final MsDatabase db;

    /**
     * Constructs a new Microsoft SQL users reader.
     *
     * @param loader the JDBC loader base for database operations
     * @param db     the Microsoft SQL database instance
     */
    public MsUsersReader(MsJdbcLoader loader, MsDatabase db) {
        super(loader);
        this.db = db;
    }

    @Override
    protected void processResult(ResultSet res) throws SQLException, XmlReaderException {
        String name = res.getString("name");
        loader.setCurrentObject(new GenericColumn(name, DbObjType.USER));

        MsUser user = new MsUser(name);
        user.setLogin(res.getString("loginname"));

        user.setSchema(res.getString("schema_name"));
        user.setLanguage(res.getString("default_lang"));
        user.setAllowEncrypted(res.getBoolean("allow_encrypted"));

        loader.setPrivileges(user, MsXmlReader.readXML(res.getString("acl")));
        db.addUser(user);
    }

    @Override
    protected void fillQueryBuilder(QueryBuilder builder) {
        addMsPrivilegesPart(builder);

        builder
                .column("res.name")
                .column("suser_sname(res.sid) AS loginname")
                .column("res.default_schema_name AS schema_name")
                .column("res.default_language_lcid AS default_lang")
                .column("res.allow_encrypted_value_modifications AS allow_encrypted")
                .from("sys.database_principals res WITH (NOLOCK)")
                .where("res.type IN ('S', 'U', 'G')")
                .where("NOT name IN ('guest', 'sys', 'INFORMATION_SCHEMA')");
    }

    @Override
    public QueryBuilder formatMsPrivileges(QueryBuilder privileges) {
        return privileges
                .column("col.name AS c")
                .where("major_id = res.principal_id")
                .where("perm.class = 4");
    }

    @Override
    public String getMsOwnerPartJoin() {
        return "JOIN ";
    }
}
