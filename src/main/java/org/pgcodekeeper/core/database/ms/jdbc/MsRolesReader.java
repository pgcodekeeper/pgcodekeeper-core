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

import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.api.schema.GenericColumn;
import org.pgcodekeeper.core.database.base.jdbc.AbstractJdbcReader;
import org.pgcodekeeper.core.database.base.jdbc.QueryBuilder;
import org.pgcodekeeper.core.database.ms.loader.MsJdbcLoader;
import org.pgcodekeeper.core.database.ms.schema.MsDatabase;
import org.pgcodekeeper.core.database.ms.schema.MsRole;
import org.pgcodekeeper.core.exception.XmlReaderException;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Reader for Microsoft SQL roles.
 * Loads role definitions from sys.database_principals system view.
 */
public class MsRolesReader extends AbstractJdbcReader<MsJdbcLoader> implements IMsJdbcReader {

    private final MsDatabase db;

    /**
     * Constructs a new Microsoft SQL roles reader.
     *
     * @param loader the JDBC loader base for database operations
     * @param db     the Microsoft SQL database instance
     */
    public MsRolesReader(MsJdbcLoader loader, MsDatabase db) {
        super(loader);
        this.db = db;
    }

    @Override
    protected void processResult(ResultSet res) throws SQLException, XmlReaderException {
        String name = res.getString("name");
        loader.setCurrentObject(new GenericColumn(name, DbObjType.ROLE));

        MsRole role = new MsRole(name);
        loader.setOwner(role, res.getString("owner"));

        for (MsXmlReader group : MsXmlReader.readXML(res.getString("groups"))) {
            role.addMember(group.getString("m"));
        }

        loader.setPrivileges(role, MsXmlReader.readXML(res.getString("acl")));

        db.addRole(role);
    }

    @Override
    protected void fillQueryBuilder(QueryBuilder builder) {
        addMsPrivilegesPart(builder);
        addMsGroupsPart(builder);
        addMsOwnerPart("res.owning_principal_id", builder);

        builder
                .column("res.name")
                .from("sys.database_principals res WITH (NOLOCK)")
                .where("res.type IN ('R')")
                .where("res.is_fixed_role = 0")
                .where("res.name != N'public'");
    }

    private void addMsGroupsPart(QueryBuilder builder) {
        QueryBuilder subSelect = new QueryBuilder()
                .column("p1.name AS m")
                .from("sys.database_role_members AS rm WITH (NOLOCK)")
                .join("INNER JOIN sys.database_principals p1 WITH (NOLOCK) ON rm.member_principal_id=p1.principal_id")
                .where("rm.role_principal_id=res.principal_id")
                .postAction("FOR XML RAW, ROOT");

        builder.column("cc.groups");
        builder.join("CROSS APPLY", subSelect, "cc (groups)");
    }

    @Override
    public QueryBuilder formatMsPrivileges(QueryBuilder privileges) {
        return privileges
                .where("major_id = res.principal_id")
                .where("perm.class = 4");
    }

    @Override
    public String getMsOwnerPartJoin() {
        return "JOIN ";
    }
}
