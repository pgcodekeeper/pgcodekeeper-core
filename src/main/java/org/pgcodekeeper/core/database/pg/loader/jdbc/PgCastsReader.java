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
package org.pgcodekeeper.core.database.pg.loader.jdbc;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.database.pg.PgDiffUtils;
import org.pgcodekeeper.core.loader.QueryBuilder;
import org.pgcodekeeper.core.loader.jdbc.AbstractStatementReader;
import org.pgcodekeeper.core.loader.jdbc.JdbcLoaderBase;
import org.pgcodekeeper.core.loader.jdbc.JdbcReader;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.base.QNameParser;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser;
import org.pgcodekeeper.core.parsers.antlr.pg.statement.PgParserAbstract;
import org.pgcodekeeper.core.database.api.schema.GenericColumn;
import org.pgcodekeeper.core.database.api.schema.ICast.CastContext;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.database.pg.schema.PgCast;
import org.pgcodekeeper.core.database.pg.schema.PgCast.CastMethod;
import org.pgcodekeeper.core.database.pg.schema.PgDatabase;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Reader for PostgreSQL casts.
 * Loads cast definitions from pg_cast system catalog.
 */
public final class PgCastsReader extends AbstractStatementReader {

    private final PgDatabase db;

    /**
     * Creates a new casts reader.
     *
     * @param loader the JDBC loader base for database operations
     * @param db     the PostgreSQL database to load casts into
     */
    public PgCastsReader(JdbcLoaderBase loader, PgDatabase db) {
        super(loader);
        this.db = db;
    }

    @Override
    protected void processResult(ResultSet res) throws SQLException {
        String source = res.getString("source");
        JdbcReader.checkTypeValidity(source);
        String target = res.getString("target");
        JdbcReader.checkTypeValidity(target);

        PgCast cast = new PgCast(source, target);
        loader.setCurrentObject(new GenericColumn(cast.getName(), DbObjType.CAST));

        addDep(cast, source);
        addDep(cast, target);

        String type = res.getString("castcontext");
        switch (type) {
            case "e":
                cast.setContext(CastContext.EXPLICIT);
                break;
            case "a":
                cast.setContext(CastContext.ASSIGNMENT);
                break;
            case "i":
                cast.setContext(CastContext.IMPLICIT);
                break;
            default:
                throw new IllegalStateException("Unknown cast context: " + type);
        }

        String method = res.getString("castmethod");
        switch (method) {
            case "f":
                cast.setMethod(CastMethod.FUNCTION);
                String function = res.getString("func");
                JdbcReader.checkObjectValidity(function, DbObjType.CAST, cast.getName());
                cast.setFunction(function);
                loader.submitAntlrTask(function, SQLParser::function_args_parser, ctx -> {
                    List<ParserRuleContext> ids = PgParserAbstract.getIdentifiers(ctx.schema_qualified_name());
                    String schemaName = QNameParser.getSchemaName(ids);
                    if (schemaName != null && !PgDiffUtils.isSystemSchema(schemaName)) {
                        String funcName = PgParserAbstract.parseSignature(
                                QNameParser.getFirstName(ids), ctx.function_args());
                        cast.addDependency(new GenericColumn(schemaName, funcName, DbObjType.FUNCTION));
                    }
                });

                break;
            case "i":
                cast.setMethod(CastMethod.INOUT);
                break;
            case "b":
                cast.setMethod(CastMethod.BINARY);
                break;
            default:
                throw new IllegalStateException("Unknown cast method: " + type);
        }

        loader.setComment(cast, res);
        loader.setAuthor(cast, res);

        db.addChild(cast);
    }

    private void addDep(AbstractStatement statement, String objectName) {
        if (objectName.indexOf('.') != -1) {
            QNameParser<ParserRuleContext> parser = QNameParser.parsePg(objectName);
            String schemaName = parser.getSchemaName();
            if (schemaName != null && !PgDiffUtils.isSystemSchema(schemaName)) {
                statement.addDependency(new GenericColumn(schemaName, parser.getFirstName(), DbObjType.TYPE));
            }
        }
    }

    @Override
    protected void setParams(PreparedStatement statement) throws SQLException {
        statement.setLong(1, loader.getLastSysOid());
    }

    @Override
    protected String getClassId() {
        return "pg_cast";
    }

    @Override
    protected void fillQueryBuilder(QueryBuilder builder) {
        addExtensionDepsCte(builder);
        addDescriptionPart(builder);

        builder
                .column("pg_catalog.format_type(res.castsource, null) AS source")
                .column("pg_catalog.format_type(res.casttarget, null) AS target")
                .column("res.castfunc::regprocedure AS func")
                .column("res.castcontext")
                .column("res.castmethod")
                .from("pg_catalog.pg_cast res")
                .where("res.oid > ?");

        if (SupportedPgVersion.VERSION_14.isLE(loader.getVersion())) {
            builder.where("NOT EXISTS (SELECT 1 FROM pg_range r WHERE res.castsource = r.rngtypid AND res.casttarget = r.rngmultitypid)");
        }
    }
}
