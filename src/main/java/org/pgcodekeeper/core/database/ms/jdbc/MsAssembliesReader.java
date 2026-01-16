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

import java.sql.*;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.jdbc.*;
import org.pgcodekeeper.core.database.ms.loader.MsJdbcLoader;
import org.pgcodekeeper.core.database.ms.parser.statement.MsCreateAssembly;
import org.pgcodekeeper.core.database.ms.schema.*;
import org.pgcodekeeper.core.exception.XmlReaderException;

/**
 * Reader for Microsoft SQL assemblies.
 * Loads assembly definitions from sys.assemblies and sys.assembly_files.
 */
public class MsAssembliesReader extends AbstractJdbcReader<MsJdbcLoader> implements IMsJdbcReader {

    private final MsDatabase db;

    /**
     * Creates a new MsAssembliesReader.
     *
     * @param loader the JDBC loader base
     * @param db     the Microsoft SQL database
     */
    public MsAssembliesReader(MsJdbcLoader loader, MsDatabase db) {
        super(loader);
        this.db = db;
    }

    @Override
    protected void processResult(ResultSet res) throws SQLException, XmlReaderException {
        String name = res.getString("name");
        loader.setCurrentObject(new GenericColumn(name, DbObjType.ASSEMBLY));

        MsAssembly ass = new MsAssembly(name);
        for (MsXmlReader bin : MsXmlReader.readXML(res.getString("binaries"))) {
            ass.addBinary(MsCreateAssembly.formatBinary(bin.getString("b")));
        }

        ass.setVisible(res.getBoolean("is_visible"));

        int i = res.getInt("permission_set");
        if (i == 2) {
            ass.setPermission("EXTERNAL_ACCESS");
        } else if (i == 3) {
            ass.setPermission("UNSAFE");
        }

        loader.setOwner(ass, res.getString("owner"));
        loader.setPrivileges(ass, MsXmlReader.readXML(res.getString("acl")));

        db.addAssembly(ass);
    }

    @Override
    protected void fillQueryBuilder(QueryBuilder builder) {
        addMsPrivilegesPart(builder);
        addMsBinariesPart(builder);
        addMsOwnerPart(builder);

        builder
                .column("res.name")
                .column("res.is_visible")
                .column("res.permission_set")
                .from("sys.assemblies res WITH (NOLOCK)")
                .where("res.is_user_defined = 1");
    }

    private void addMsBinariesPart(QueryBuilder builder) {
        QueryBuilder binaries = new QueryBuilder()
                .column("convert(varchar(max), af.content, 1) b")
                .from("sys.assembly_files af WITH (NOLOCK)")
                .where("res.assembly_id = af.assembly_id")
                .where("af.assembly_id > 65535")
                .postAction("FOR XML RAW, ROOT");

        builder.column("bb.binaries");
        builder.join("CROSS APPLY", binaries, "bb (binaries)");
    }

    @Override
    public QueryBuilder formatMsPrivileges(QueryBuilder privileges) {
        return privileges
                .where("major_id = res.assembly_id")
                .where("perm.class = 5");
    }

    @Override
    public String getMsOwnerPartJoin() {
        return "JOIN ";
    }
}
