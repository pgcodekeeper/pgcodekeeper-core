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
package org.pgcodekeeper.core.loader.jdbc.ms;

import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.loader.QueryBuilder;
import org.pgcodekeeper.core.loader.jdbc.AbstractStatementReader;
import org.pgcodekeeper.core.loader.jdbc.JdbcLoaderBase;
import org.pgcodekeeper.core.loader.jdbc.XmlReader;
import org.pgcodekeeper.core.loader.jdbc.XmlReaderException;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.base.schema.AbstractSchema;
import org.pgcodekeeper.core.database.api.schema.GenericColumn;
import org.pgcodekeeper.core.database.ms.schema.MsDatabase;
import org.pgcodekeeper.core.database.ms.schema.MsSchema;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Reader for Microsoft SQL schemas.
 * Loads schema definitions from sys.schemas system view.
 */
public class MsSchemasReader extends AbstractStatementReader {

    private final MsDatabase db;

    /**
     * Constructs a new Microsoft SQL schemas reader.
     *
     * @param loader the JDBC loader base for database operations
     * @param db     the Microsoft SQL database instance
     */
    public MsSchemasReader(JdbcLoaderBase loader, MsDatabase db) {
        super(loader);
        this.db = db;
    }

    @Override
    protected void processResult(ResultSet res) throws SQLException, XmlReaderException {
        String schemaName = res.getString("name");
        loader.setCurrentObject(new GenericColumn(schemaName, DbObjType.SCHEMA));
        if (loader.isIgnoredSchema(schemaName)) {
            return;
        }

        AbstractSchema schema = new MsSchema(schemaName);
        String owner = res.getString("owner");
        if (!schemaName.equalsIgnoreCase(Consts.DBO) || !owner.equalsIgnoreCase(Consts.DBO)) {
            loader.setOwner(schema, owner);
        }

        loader.setPrivileges(schema, XmlReader.readXML(res.getString("acl")));
        loader.putSchema(res.getInt("schema_id"), schema);

        db.addSchema(schema);
    }

    @Override
    protected void fillQueryBuilder(QueryBuilder builder) {
        addMsPriviligesPart(builder);
        addMsOwnerPart(builder);

        builder
                .column("res.schema_id")
                .column("res.name")
                .from("sys.schemas res WITH (NOLOCK)")
                .where("p.name NOT IN ('INFORMATION_SCHEMA', 'sys')");
    }

    @Override
    protected QueryBuilder formatMsPriviliges(QueryBuilder privileges) {
        return privileges
                .where("major_id = res.schema_id")
                .where("perm.class = 3");
    }
}
