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
import org.pgcodekeeper.core.database.pg.parser.statement.PgCreateIndex;
import org.pgcodekeeper.core.database.pg.schema.*;
import org.pgcodekeeper.core.utils.Pair;

/**
 * Reader for PostgreSQL indices.
 * Loads index definitions from pg_class and related system catalogs.
 */
public final class PgIndicesReader extends PgAbstractSearchPathJdbcReader {

    /**
     * Creates a new PgIndicesReader.
     *
     * @param loader the JDBC loader instance
     */
    public PgIndicesReader(PgJdbcLoader loader) {
        super(loader);
    }

    @Override
    protected void processResult(ResultSet res, ISchema schema) throws SQLException {
        String tableName = res.getString("table_name");
        var cont = schema.getStatementContainer(tableName);
        if (cont == null) {
            return;
        }
        String schemaName = schema.getName();
        String indexName = res.getString("relname");
        loader.setCurrentObject(new ObjectReference(schemaName, indexName, DbObjType.INDEX));
        PgIndex i = new PgIndex(indexName);

        String tablespace = res.getString("spcname");
        String definition = res.getString("definition");
        IPgJdbcReader.checkObjectValidity(definition, DbObjType.INDEX, indexName);
        loader.submitAntlrTask(definition,
                p -> new Pair<>(p.sql().statement(0).schema_statement().schema_create().create_index_statement().index_rest(),
                        (CommonTokenStream) p.getTokenStream()),
                pair -> PgCreateIndex.parseIndex(pair.getFirst(), tablespace, schemaName, tableName, i,
                        (PgDatabase) schema.getDatabase(), loader.getCurrentLocation(), pair.getSecond(),
                        loader.getSettings()));
        loader.setAuthor(i, res);
        loader.setComment(i, res);

        i.setClustered(res.getBoolean("indisclustered"));
        i.setUnique(res.getBoolean("indisunique"));

        if (PgSupportedVersion.VERSION_15.isLE(loader.getVersion())) {
            i.setNullsDistinction(res.getBoolean("indnullsnotdistinct"));
        }

        String inhnspname = res.getString("inhnspname");
        if (inhnspname != null) {
            String inhrelname = res.getString("inhrelname");
            i.addInherit(inhnspname, inhrelname);
            addDep(i, inhnspname, inhrelname, DbObjType.INDEX);
        }
        cont.addChild(i);
    }

    @Override
    public String getClassId() {
        return "pg_class";
    }

    @Override
    protected String getSchemaColumn() {
        return "res.relnamespace";
    }

    @Override
    protected void fillQueryBuilder(QueryBuilder builder) {
        addExtensionSchemasCte(builder);
        addDescriptionPart(builder, true);

        builder
                .column("res.relname")
                .column("clsrel.relname AS table_name")
                .column("ind.indisunique")
                .column("ind.indisclustered")
                .column("t.spcname")
                .column("pg_catalog.pg_get_indexdef(res.oid) AS definition")
                .column("inhns.nspname AS inhnspname")
                .column("inhrel.relname AS inhrelname")
                .from("pg_catalog.pg_class res")
                .join("JOIN pg_catalog.pg_index ind ON res.oid = ind.indexrelid")
                .join("JOIN pg_catalog.pg_class clsrel ON clsrel.oid = ind.indrelid")
                .join("LEFT JOIN pg_catalog.pg_tablespace t ON res.reltablespace = t.oid")
                .join("LEFT JOIN pg_catalog.pg_constraint cons ON cons.conindid = ind.indexrelid AND cons.contype IN ('p', 'u', 'x')")
                .join("LEFT JOIN pg_catalog.pg_inherits inh ON (inh.inhrelid = ind.indexrelid)")
                .join("LEFT JOIN pg_catalog.pg_class inhrel ON (inh.inhparent = inhrel.oid)")
                .join("LEFT JOIN pg_catalog.pg_namespace inhns ON inhrel.relnamespace = inhns.oid")
                .where("res.relkind IN ('i', 'I')")
                .where("ind.indisprimary = FALSE")
                .where("ind.indisexclusion = FALSE")
                .where("cons.conindid is NULL");

        if (PgSupportedVersion.VERSION_15.isLE(loader.getVersion())) {
            builder.column("ind.indnullsnotdistinct");
        }
    }

    private void addExtensionSchemasCte(QueryBuilder builder) {
        builder.with(EXTENSIONS_SCHEMAS, EXTENSION_SCHEMA_CTE);
        builder.where(getSchemaColumn() + " NOT IN (SELECT oid FROM extensions_schemas)");
    }
}