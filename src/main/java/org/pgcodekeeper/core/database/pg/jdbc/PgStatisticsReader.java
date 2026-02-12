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

import org.antlr.v4.runtime.CommonTokenStream;
import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.jdbc.QueryBuilder;
import org.pgcodekeeper.core.database.pg.loader.PgJdbcLoader;
import org.pgcodekeeper.core.database.pg.parser.statement.PgCreateStatistics;
import org.pgcodekeeper.core.database.pg.schema.*;
import org.pgcodekeeper.core.utils.Pair;

/**
 * Reader for PostgreSQL extended statistics.
 * Loads extended statistics definitions from pg_statistic_ext system catalog.
 */
public final class PgStatisticsReader extends PgAbstractSearchPathJdbcReader {

    /**
     * Creates a new PgStatisticsReader.
     *
     * @param loader the JDBC loader base for database operations
     */
    public PgStatisticsReader(PgJdbcLoader loader) {
        super(loader);
    }

    @Override
    protected void processResult(ResultSet res, ISchema schema) throws SQLException {
        String schemaName = schema.getName();
        String statisticsName = res.getString("stxname");
        loader.setCurrentObject(new GenericColumn(schemaName, statisticsName, DbObjType.STATISTICS));

        PgStatistics stat = new PgStatistics(statisticsName);

        String definition = res.getString("def");
        IPgJdbcReader.checkObjectValidity(definition, DbObjType.STATISTICS, statisticsName);
        loader.submitAntlrTask(definition + ';',
                p -> new Pair<>(p.sql().statement(0).schema_statement().schema_create()
                        .create_statistics_statement(), (CommonTokenStream) p.getTokenStream()),
                pair -> new PgCreateStatistics(pair.getFirst(), (PgDatabase) schema.getDatabase(), pair.getSecond(),
                        loader.getSettings())
                        .parseStatistics(stat));

        if (PgSupportedVersion.VERSION_14.isLE(loader.getVersion())) {
            var statVal = res.getString("stxstattarget");
            if (null != statVal) {
                stat.setStatistics(Integer.parseInt(statVal));
            }
        }

        loader.setOwner(stat, res.getLong("stxowner"));
        loader.setAuthor(stat, res);
        loader.setComment(stat, res);

        schema.addChild(stat);
    }

    @Override
    public String getClassId() {
        return "pg_statistic_ext";
    }

    @Override
    protected String getSchemaColumn() {
        return "res.stxnamespace";
    }

    @Override
    protected void fillQueryBuilder(QueryBuilder builder) {
        addExtensionSchemasCte(builder);
        addDescriptionPart(builder);

        builder
                .column("res.stxname")
                .column("res.stxowner")
                .column("pg_catalog.pg_get_statisticsobjdef(res.oid::pg_catalog.oid) AS def")
                .from("pg_catalog.pg_statistic_ext res");

        if (PgSupportedVersion.VERSION_14.isLE(loader.getVersion())) {
            builder.column("res.stxstattarget");
        }
    }

    private void addExtensionSchemasCte(QueryBuilder builder) {
        builder.with(EXTENSIONS_SCHEMAS, EXTENSION_SCHEMA_CTE);
        builder.where(getSchemaColumn() + " NOT IN (SELECT oid FROM extensions_schemas)");
    }
}
