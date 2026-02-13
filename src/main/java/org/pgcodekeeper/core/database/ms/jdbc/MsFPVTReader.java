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
import java.util.List;

import org.antlr.v4.runtime.CommonTokenStream;
import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.jdbc.*;
import org.pgcodekeeper.core.database.ms.loader.MsJdbcLoader;
import org.pgcodekeeper.core.database.ms.parser.generated.TSQLParser.Batch_statementContext;
import org.pgcodekeeper.core.database.ms.parser.statement.*;
import org.pgcodekeeper.core.database.ms.schema.*;
import org.pgcodekeeper.core.exception.XmlReaderException;
import org.pgcodekeeper.core.settings.ISettings;

/**
 * Reader for Microsoft SQL functions, procedures, views, and triggers.
 * Loads function, procedure, view, and trigger definitions from sys.objects and sys.sql_modules.
 */
public final class MsFPVTReader extends AbstractSearchPathJdbcReader<MsJdbcLoader> implements IMsJdbcReader {

    /**
     * Creates a new MsFPVTReader.
     *
     * @param loader the JDBC loader base
     */
    public MsFPVTReader(MsJdbcLoader loader) {
        super(loader);
    }

    @Override
    protected void processResult(ResultSet res, ISchema schema) throws SQLException, XmlReaderException {
        String name = res.getString("name");

        String type = res.getString("type");

        // TR - SQL trigger
        // V - View
        // P - SQL Stored Procedure
        // IF - SQL inline table-valued function
        // FN - SQL scalar function
        // TF - SQL table-valued-function

        DbObjType tt = switch (type) {
            case "TR" -> DbObjType.TRIGGER;
            case "V " -> DbObjType.VIEW;
            case "P " -> DbObjType.PROCEDURE;
            default -> DbObjType.FUNCTION;
        };

        loader.setCurrentObject(new ObjectReference(schema.getName(), name, tt));
        boolean an = res.getBoolean("ansi_nulls");
        boolean qi = res.getBoolean("quoted_identifier");
        boolean isDisable = res.getBoolean("is_disabled");

        String def = res.getString("definition");
        String owner = res.getString("owner");

        List<MsXmlReader> acls = MsXmlReader.readXML(res.getString("acl"));

        MsDatabase db = (MsDatabase) schema.getDatabase();

        ISettings settings = loader.getSettings();
        if (tt == DbObjType.TRIGGER) {
            loader.submitMsAntlrTask(def, p -> {
                Batch_statementContext ctx = p.tsql_file().batch(0).batch_statement();
                return new MsCreateTrigger(ctx, db, an, qi, (CommonTokenStream) p.getInputStream(), settings);
            }, creator -> {
                MsTrigger tr = creator.getObject((MsSchema) schema, true);
                tr.setDisable(isDisable);
            });
        } else if (tt == DbObjType.VIEW) {
            MsView view = new MsView(name);
            schema.addChild(view);
            loader.setOwner(view, owner);
            loader.setPrivileges(view, acls);

            loader.submitMsAntlrTask(def, p -> {
                Batch_statementContext ctx = p.tsql_file().batch(0).batch_statement();
                return new MsCreateView(ctx, db, an, qi, (CommonTokenStream) p.getInputStream(), settings);
            }, creator -> creator.fillObject(view));
        } else if (tt == DbObjType.PROCEDURE) {
            loader.submitMsAntlrTask(def, p -> {
                Batch_statementContext ctx = p.tsql_file().batch(0).batch_statement();
                return new MsCreateProcedure(ctx, db, an, qi, (CommonTokenStream) p.getInputStream(), settings);
            }, creator -> {
                MsAbstractCommonFunction st = creator.getObject((MsSchema) schema, true);
                loader.setOwner(st, owner);
                loader.setPrivileges(st, acls);
            });
        } else {
            loader.submitMsAntlrTask(def, p -> {
                Batch_statementContext ctx = p.tsql_file().batch(0).batch_statement();
                return new MsCreateFunction(ctx, db, an, qi, (CommonTokenStream) p.getInputStream(), settings);
            }, creator -> {
                MsAbstractCommonFunction st = creator.getObject((MsSchema) schema, true);
                loader.setOwner(st, owner);
                loader.setPrivileges(st, acls);
            });
        }
    }

    @Override
    protected String getSchemaColumn() {
        return "res.schema_id";
    }

    @Override
    protected void fillQueryBuilder(QueryBuilder builder) {
        addMsPrivilegesPart(builder);
        addMsOwnerPart(builder);

        builder
                .column("res.name")
                .column("res.type")
                .column("sm.definition")
                .column("sm.uses_ansi_nulls AS ansi_nulls")
                .column("sm.uses_quoted_identifier AS quoted_identifier")
                .column("t.is_disabled")
                .from("sys.objects res WITH (NOLOCK)")
                .join("JOIN sys.sql_modules sm WITH (NOLOCK) ON sm.object_id=res.object_id")
                .join("LEFT JOIN sys.triggers t WITH (NOLOCK) ON t.object_id=res.object_id")
                .where("res.type IN (N'TR', N'V', N'IF', N'FN', N'TF', N'P')")
                .where("definition IS NOT NULL");
    }
}
