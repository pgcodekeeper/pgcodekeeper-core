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
package org.pgcodekeeper.core.database.pg.jdbc;

import java.sql.*;
import java.util.List;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.api.schema.ICast.CastContext;
import org.pgcodekeeper.core.database.base.jdbc.QueryBuilder;
import org.pgcodekeeper.core.database.base.parser.QNameParser;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.database.pg.PgDiffUtils;
import org.pgcodekeeper.core.database.pg.loader.PgJdbcLoader;
import org.pgcodekeeper.core.database.pg.parser.PgParserUtils;
import org.pgcodekeeper.core.database.pg.parser.generated.SQLParser;
import org.pgcodekeeper.core.database.pg.parser.statement.PgParserAbstract;
import org.pgcodekeeper.core.database.pg.schema.*;
import org.pgcodekeeper.core.database.pg.schema.PgCast.CastMethod;

/**
 * Reader for PostgreSQL casts.
 * Loads cast definitions from pg_cast system catalog.
 */
public final class PgCastsReader extends PgAbstractJdbcReader {

    private final PgDatabase db;

    /**
     * Creates a new casts reader.
     *
     * @param loader the JDBC loader base for database operations
     * @param db     the PostgreSQL database to load casts into
     */
    public PgCastsReader(PgJdbcLoader loader, PgDatabase db) {
        super(loader);
        this.db = db;
    }

    @Override
    protected void processResult(ResultSet res) throws SQLException {
        String source = res.getString("source");
        IPgJdbcReader.checkTypeValidity(source);
        String target = res.getString("target");
        IPgJdbcReader.checkTypeValidity(target);

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
                IPgJdbcReader.checkObjectValidity(function, DbObjType.CAST, cast.getName());
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
            QNameParser<ParserRuleContext> parser = PgParserUtils.parseQName(objectName);
            String schemaName = parser.getSchemaName();
            if (schemaName != null && !PgDiffUtils.isSystemSchema(schemaName)) {
                statement.addDependency(new GenericColumn(schemaName, parser.getFirstName(), DbObjType.TYPE));
            }
        }
    }

    @Override
    protected void setQueryParams(PreparedStatement statement) throws SQLException {
        statement.setLong(1, loader.getLastSysOid());
    }

    @Override
    public String getClassId() {
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
