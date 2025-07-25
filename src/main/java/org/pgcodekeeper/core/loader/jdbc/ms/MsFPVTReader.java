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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.antlr.v4.runtime.CommonTokenStream;
import org.pgcodekeeper.core.loader.QueryBuilder;
import org.pgcodekeeper.core.loader.jdbc.JdbcLoaderBase;
import org.pgcodekeeper.core.loader.jdbc.JdbcReader;
import org.pgcodekeeper.core.loader.jdbc.XmlReader;
import org.pgcodekeeper.core.loader.jdbc.XmlReaderException;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.generated.TSQLParser.Batch_statementContext;
import org.pgcodekeeper.core.parsers.antlr.statements.ms.CreateMsFunction;
import org.pgcodekeeper.core.parsers.antlr.statements.ms.CreateMsProcedure;
import org.pgcodekeeper.core.parsers.antlr.statements.ms.CreateMsTrigger;
import org.pgcodekeeper.core.parsers.antlr.statements.ms.CreateMsView;
import org.pgcodekeeper.core.schema.AbstractFunction;
import org.pgcodekeeper.core.schema.AbstractSchema;
import org.pgcodekeeper.core.schema.GenericColumn;
import org.pgcodekeeper.core.schema.ms.MsDatabase;
import org.pgcodekeeper.core.schema.ms.MsTrigger;
import org.pgcodekeeper.core.schema.ms.MsView;
import org.pgcodekeeper.core.settings.ISettings;

public final class MsFPVTReader extends JdbcReader {

    public MsFPVTReader(JdbcLoaderBase loader) {
        super(loader);
    }

    @Override
    protected void processResult(ResultSet res, AbstractSchema schema) throws SQLException, XmlReaderException {
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

        loader.setCurrentObject(new GenericColumn(schema.getName(), name, tt));
        boolean an = res.getBoolean("ansi_nulls");
        boolean qi = res.getBoolean("quoted_identifier");
        boolean isDisable = res.getBoolean("is_disabled");

        String def = res.getString("definition");
        String owner = res.getString("owner");

        List<XmlReader> acls = XmlReader.readXML(res.getString("acl"));

        MsDatabase db = (MsDatabase) schema.getDatabase();

        ISettings settings = loader.getSettings();
        if (tt == DbObjType.TRIGGER) {
            loader.submitMsAntlrTask(def, p -> {
                Batch_statementContext ctx = p.tsql_file().batch(0).batch_statement();
                return new CreateMsTrigger(ctx, db, an, qi, (CommonTokenStream) p.getInputStream(), settings);
            }, creator -> {
                MsTrigger tr = creator.getObject(schema, true);
                tr.setDisable(isDisable);
            });
        } else if (tt == DbObjType.VIEW) {
            MsView view = new MsView(name);
            schema.addChild(view);
            loader.setOwner(view, owner);
            loader.setPrivileges(view, acls);

            loader.submitMsAntlrTask(def, p -> {
                Batch_statementContext ctx = p.tsql_file().batch(0).batch_statement();
                return new CreateMsView(ctx, db, an, qi, (CommonTokenStream) p.getInputStream(), settings);
            }, creator -> creator.fillObject(view));
        } else if (tt == DbObjType.PROCEDURE) {
            loader.submitMsAntlrTask(def, p -> {
                Batch_statementContext ctx = p.tsql_file().batch(0).batch_statement();
                return new CreateMsProcedure(ctx, db, an, qi, (CommonTokenStream) p.getInputStream(), settings);
            }, creator -> {
                AbstractFunction st = creator.getObject(schema, true);
                loader.setOwner(st, owner);
                loader.setPrivileges(st, acls);
            });
        } else {
            loader.submitMsAntlrTask(def, p -> {
                Batch_statementContext ctx = p.tsql_file().batch(0).batch_statement();
                return new CreateMsFunction(ctx, db, an, qi, (CommonTokenStream) p.getInputStream(), settings);
            }, creator -> {
                AbstractFunction st = creator.getObject(schema, true);
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
        addMsPriviligesPart(builder);
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
