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

import org.antlr.v4.runtime.CommonTokenStream;
import org.pgcodekeeper.core.database.pg.loader.PgJdbcLoader;
import org.pgcodekeeper.core.database.pg.schema.*;
import org.pgcodekeeper.core.database.base.jdbc.QueryBuilder;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.base.statement.ParserAbstract;
import org.pgcodekeeper.core.parsers.antlr.pg.statement.AlterTable;
import org.pgcodekeeper.core.database.base.schema.AbstractSchema;
import org.pgcodekeeper.core.database.api.schema.GenericColumn;
import org.pgcodekeeper.core.utils.Pair;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Reader for PostgreSQL constraints.
 * Loads constraint definitions from pg_constraint system catalog.
 */
public final class PgConstraintsReader extends PgAbstractSearchPathJdbcReader {

    private static final String ADD_CONSTRAINT = "ALTER TABLE noname ADD CONSTRAINT noname ";

    /**
     * Creates a new constraints reader.
     *
     * @param loader the JDBC loader base for database operations
     */
    public PgConstraintsReader(PgJdbcLoader loader) {
        super(loader);
    }

    @Override
    protected void processResult(ResultSet res, AbstractSchema schema) throws SQLException {
        if (SupportedPgVersion.GP_VERSION_7.isLE(loader.getVersion()) && res.getInt("conparentid") != 0) {
            return;
        }

        String tableName = res.getString("relname");
        var cont = schema.getStatementContainer(tableName);
        if (cont == null) {
            return;
        }

        String schemaName = schema.getName();
        String constraintName = res.getString("conname");
        String[] params = PgJdbcUtils.getColArray(res, "reloptions", true);
        loader.setCurrentObject(new GenericColumn(schemaName, tableName, constraintName, DbObjType.CONSTRAINT));
        PgConstraint constr;

        String type = res.getString("contype");
        switch (type) {
            case "p", "u":
                constr = new PgConstraintPk(constraintName, "p".equals(type));
                ((PgConstraintPk) constr).setClustered(res.getBoolean("isclustered"));
                break;
            case "f":
                constr = new PgConstraintFk(constraintName);
                break;
            case "c":
                constr = new PgConstraintCheck(constraintName);
                break;
            case "x":
                constr = new PgConstraintExclude(constraintName);
                break;
            case "n":
                readNotNullConstraint(res, (PgAbstractTable) cont, constraintName);
                return;
            default:
                throw new IllegalArgumentException("Unsupported constraint's type " + type);
        }

        if (params != null) {
            ParserAbstract.fillOptionParams(params, ((PgIndexParamContainer) constr)::addParam, false, false, false);
        }

        String definition = res.getString("definition");
        IPgJdbcReader.checkObjectValidity(definition, DbObjType.CONSTRAINT, constraintName);
        String tablespace = res.getString("spcname");
        loader.submitAntlrTask(ADD_CONSTRAINT + definition + ';',
                p -> new Pair<>(p.sql().statement(0).schema_statement().schema_alter()
                        .alter_table_statement().table_action(0), (CommonTokenStream) p.getTokenStream()),
                pair -> new AlterTable(null, (PgDatabase) schema.getDatabase(), tablespace, pair.getSecond(),
                        loader.getSettings())
                        .parseAlterTableConstraint(
                                pair.getFirst(), constr, schemaName, tableName, loader.getCurrentLocation()));
        loader.setAuthor(constr, res);
        loader.setComment(constr, res);

        cont.addConstraint(constr);
    }

    private void readNotNullConstraint(ResultSet res, PgAbstractTable table,
            String constraintName) throws SQLException {
        String columnName = res.getString("col_name");
        PgColumn column = (PgColumn) table.getColumn(columnName);
        if (column == null) {
            if (table.hasInherits()) {
                column = new PgColumn(columnName);
                if (!(table instanceof PgPartitionTable)) {
                    column.setInherit(true);
                }
                table.addColumn(column);
            } else {
                return;
            }
        }

        var notNullConstraint = new PgConstraintNotNull(constraintName);
        notNullConstraint.setNoInherit(res.getBoolean("connoinherit"));
        notNullConstraint.setNotValid(!res.getBoolean("convalidated"));
        loader.setComment(notNullConstraint, res);

        column.setNotNullConstraint(notNullConstraint);
        notNullConstraint.setParent(column);
    }

    @Override
    public String getClassId() {
        return "pg_constraint";
    }

    @Override
    protected String getSchemaColumn() {
        return "ccc.relnamespace";
    }

    @Override
    protected void fillQueryBuilder(QueryBuilder builder) {
        addExtensionSchemasCte(builder);
        addDescriptionPart(builder);

        builder
                .column("ccc.relname")
                .column("res.conname")
                .column("res.contype")
                .column("ts.spcname")
                .column("ci.reloptions")
                .column("pg_catalog.pg_get_constraintdef(res.oid) AS definition")
                .column("idx.indisclustered as isclustered")
                .from("pg_catalog.pg_constraint res")
                .join("LEFT JOIN pg_catalog.pg_class ccc ON ccc.oid = res.conrelid")
                .join("LEFT JOIN pg_catalog.pg_class cf ON cf.oid = res.confrelid")
                .join("LEFT JOIN pg_catalog.pg_class ci ON ci.oid = res.conindid AND res.contype != 'f'")
                .join("LEFT JOIN pg_catalog.pg_tablespace ts ON ts.oid = ci.reltablespace")
                .join("LEFT JOIN pg_catalog.pg_index idx ON idx.indexrelid = ci.oid")
                .where("ccc.relkind IN ('r', 'p', 'f')")
                .where("res.contype NOT IN ('t')")
                .where("res.coninhcount = 0");

        if (SupportedPgVersion.GP_VERSION_7.isLE(loader.getVersion())) {
            builder.column("res.conparentid::bigint");
        }

        if (SupportedPgVersion.VERSION_18.isLE(loader.getVersion())) {
            builder
                    .column("a.attname AS col_name")
                    .column("res.connoinherit")
                    .column("res.convalidated")
                    .join("LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = res.conrelid AND a.attnum = res.conkey[1]");
        }
    }

    private void addExtensionSchemasCte(QueryBuilder builder) {
        builder.with(EXTENSIONS_SCHEMAS, EXTENSION_SCHEMA_CTE);
        builder.where(getSchemaColumn() + " NOT IN (SELECT oid FROM extensions_schemas)");
    }
}
