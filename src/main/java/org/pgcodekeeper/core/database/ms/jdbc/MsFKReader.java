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
import org.pgcodekeeper.core.database.ms.schema.MsConstraintFk;
import org.pgcodekeeper.core.database.ms.schema.MsTable;
import org.pgcodekeeper.core.exception.XmlReaderException;

/**
 * Reader for Microsoft SQL foreign key constraints.
 * Loads foreign key constraint definitions from sys.foreign_keys and sys.foreign_key_columns.
 */
public class MsFKReader extends AbstractSearchPathJdbcReader<MsJdbcLoader> implements IMsJdbcReader {

    /**
     * Creates a new MsFKReader.
     *
     * @param loader the JDBC loader base
     */
    public MsFKReader(MsJdbcLoader loader) {
        super(loader);
    }

    @Override
    protected void processResult(ResultSet res, ISchema schema)
            throws SQLException, XmlReaderException {
        String name = res.getString("name");
        loader.setCurrentObject(new ObjectReference(schema.getName(), name, DbObjType.CONSTRAINT));

        var constrFk = new MsConstraintFk(name);

        constrFk.setNotValid(res.getBoolean("with_no_check"));
        constrFk.setDisabled(res.getBoolean("is_disabled"));

        String fSchemaName = res.getString("referenced_schema_name");
        String fTableName = res.getString("referenced_table_name");

        constrFk.setForeignSchema(fSchemaName);
        constrFk.setForeignTable(fTableName);
        constrFk.addDependency(new ObjectReference(fSchemaName, fTableName, DbObjType.TABLE));

        for (MsXmlReader col : MsXmlReader.readXML(res.getString("cols"))) {
            String field = col.getString("f");
            String fCol = col.getString("r");

            constrFk.addForeignColumn(fCol);
            constrFk.addDependency(new ObjectReference(fSchemaName, fTableName, fCol, DbObjType.COLUMN));
            constrFk.addColumn(field);
        }

        constrFk.setDelAction(getAction(res.getInt("delete_referential_action")));
        constrFk.setUpdAction(getAction(res.getInt("update_referential_action")));
        constrFk.setNotForRepl(res.getBoolean("is_not_for_replication"));
        MsTable table = (MsTable) schema.getChild(res.getString("table_name"), DbObjType.TABLE);;
        table.addChild(constrFk);
    }

    private String getAction(int value) {
        if (value <= 0) {
            return null;
        }
        if (value == 1) {
            return "CASCADE";
        }
        return value == 2 ? "SET NULL" : "SET DEFAULT";
    }

    @Override
    protected void fillQueryBuilder(QueryBuilder builder) {
        addMsColsPart(builder);

        builder
                .column("fs.name AS table_name")
                .column("res.name")
                .column("SCHEMA_NAME(rs.schema_id) AS referenced_schema_name")
                .column("rs.name AS referenced_table_name")
                .column("res.is_disabled")
                .column("res.is_not_for_replication")
                .column("res.update_referential_action")
                .column("res.is_not_trusted AS with_no_check")
                .column("res.delete_referential_action")
                .from("sys.foreign_keys res WITH (NOLOCK)")
                .join("JOIN sys.objects fs WITH (NOLOCK) ON res.parent_object_id=fs.object_id")
                .join("JOIN sys.objects rs WITH (NOLOCK) ON res.referenced_object_id=rs.object_id");
    }

    protected void addMsColsPart(QueryBuilder builder) {
        QueryBuilder subSelect = new QueryBuilder()
                .column("fc.name AS f")
                .column("rc.name AS r")
                .from("sys.foreign_key_columns AS sfkc WITH (NOLOCK)")
                .join("JOIN sys.columns fc WITH (NOLOCK) ON sfkc.parent_column_id=fc.column_id AND fc.object_id=sfkc.parent_object_id")
                .join("JOIN sys.columns rc WITH (NOLOCK) ON sfkc.referenced_column_id=rc.column_id AND rc.object_id=sfkc.referenced_object_id")
                .where("res.object_id=sfkc.constraint_object_id")
                .orderBy("sfkc.constraint_column_id")
                .postAction("FOR XML RAW, ROOT");

        builder.column("aa.cols");
        builder.join("CROSS APPLY", subSelect, "aa (cols)");
    }

    @Override
    protected String getSchemaColumn() {
        return "fs.schema_id";
    }
}
