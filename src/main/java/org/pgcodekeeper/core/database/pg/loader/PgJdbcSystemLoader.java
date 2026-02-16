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
package org.pgcodekeeper.core.database.pg.loader;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.stream.IntStream;

import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.database.api.jdbc.IJdbcConnector;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.api.schema.ICast.CastContext;
import org.pgcodekeeper.core.database.base.jdbc.QueryBuilder;
import org.pgcodekeeper.core.database.base.parser.statement.ParserAbstract;
import org.pgcodekeeper.core.database.base.schema.Argument;
import org.pgcodekeeper.core.database.base.schema.meta.*;
import org.pgcodekeeper.core.database.pg.PgDiffUtils;
import org.pgcodekeeper.core.database.pg.jdbc.*;
import org.pgcodekeeper.core.database.pg.parser.generated.SQLParser.*;
import org.pgcodekeeper.core.database.pg.schema.PgDatabase;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.monitor.*;
import org.pgcodekeeper.core.settings.DiffSettings;
import org.pgcodekeeper.core.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC-based system metadata loader for PostgreSQL databases.
 * Loads system functions, relations, operators, and casts from PostgreSQL system catalogs
 * to build metadata storage for analysis purposes.
 */
final class PgJdbcSystemLoader extends PgJdbcLoader {

    private static final Logger LOG = LoggerFactory.getLogger(PgJdbcSystemLoader.class);

    private static final String NSPNAME = "n.nspname";
    private static final String SYSTEM_SCHEMAS = "(n.nspname LIKE 'pg\\_%' OR n.nspname = 'information_schema')";

    private static final String QUERY_SYSTEM_RELATIONS = new QueryBuilder()
            .column("c.relname AS name")
            .column("c.relkind")
            .column(NSPNAME)
            .column("columns.col_names")
            .column("columns.col_types")
            .from("pg_catalog.pg_class c")
            .join("LEFT JOIN (SELECT"
                    + "   a.attrelid,"
                    + "   pg_catalog.array_agg(a.attname ORDER BY a.attnum) AS col_names,"
                    + "   pg_catalog.array_agg(pg_catalog.format_type(a.atttypid, a.atttypmod) ORDER BY a.attnum) AS col_types"
                    + " FROM pg_catalog.pg_attribute a"
                    + " WHERE a.attisdropped IS FALSE AND a.attnum > 0"
                    + " GROUP BY attrelid) columns ON columns.attrelid = c.oid")
            .join("LEFT JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid")
            .where("c.relkind IN ('f','r','p', 'v', 'm', 'S')")
            .where(SYSTEM_SCHEMAS)
            .build();

    private static final String QUERY_SYSTEM_FUNCTIONS = new QueryBuilder()
            .column("p.proname AS name")
            .column(NSPNAME)
            .column("p.proargmodes")
            .column("p.proretset")
            .column("p.proargnames")
            .column("pg_catalog.format_type(p.prorettype, null) AS prorettype")
            .column("p.proallargtypes::bigint[]")
            .column("pg_catalog.pg_get_function_arguments(p.oid) AS proarguments")
            .from("pg_catalog.pg_proc p")
            .join("LEFT JOIN pg_catalog.pg_namespace n ON p.pronamespace = n.oid")
            .where("NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend dp"
                    + " WHERE dp.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass"
                    + " AND dp.objid = p.oid AND dp.deptype = 'i')")
            .where(SYSTEM_SCHEMAS)
            .build();

    private static final String QUERY_SYSTEM_OPERATORS =  new QueryBuilder()
            .column("o.oprname as name")
            .column(NSPNAME)
            .column("o.oprleft::bigint AS left")
            .column("o.oprright::bigint AS right")
            .column("o.oprresult::bigint AS result")
            .from("pg_catalog.pg_operator o")
            .join("LEFT JOIN pg_catalog.pg_namespace n ON o.oprnamespace = n.oid")
            .where(SYSTEM_SCHEMAS)
            .build();

    private static final String QUERY_SYSTEM_CASTS = new QueryBuilder()
            .column("pg_catalog.format_type(c.castsource, null) AS source")
            .column("pg_catalog.format_type(c.casttarget, null) AS target")
            .column("c.castcontext")
            .from("pg_catalog.pg_cast c")
            .where("c.oid <= ?")
            .build();

    private static final String NAMESPACE_NAME = "nspname";
    private static final String NAME = "name";

    /**
     * Creates a new system loader for the specified database connection.
     *
     * @param connector the JDBC connector for database connection
     */
    private PgJdbcSystemLoader(IJdbcConnector connector) {
        super(connector, Consts.UTC, new DiffSettings(null));
    }

    /**
     * Not supported operation for system loader.
     *
     * @throws IllegalStateException always, as this operation is not supported
     */
    @Override
    public PgDatabase load() {
        throw new IllegalStateException("Unsupported operation for JdbcSystemLoader");
    }

    /**
     * Loads system metadata from JDBC connection and returns metadata storage.
     * Reads system functions, relations, operators, and casts from PostgreSQL catalogs.
     *
     * @return metadata storage containing system objects
     * @throws IOException          if database access fails
     * @throws InterruptedException if loading is interrupted
     */
    public MetaStorage getStorageFromJdbc() throws IOException, InterruptedException {
        MetaStorage storage = new MetaStorage();
        LOG.info(Messages.JdbcLoader_log_reading_db_jdbc);
        setCurrentOperation(Messages.JdbcChLoader_log_connection_db);
        try (Connection connection = connector.getConnection();
             Statement statement = connection.createStatement()) {
            this.connection = connection;
            this.statement = statement;
            connection.setAutoCommit(false);
            getRunner().run(statement, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY");
            getRunner().run(statement, "SET search_path TO pg_catalog;");
            getRunner().run(statement, "SET timezone = " + Utils.quoteString(timezone));

            queryCheckGreenplumDb();
            queryCheckPgVersion();
            queryCheckLastSysOid();
            queryTypesForCache();

            readRelations(storage);
            readFunctions(storage);
            readOperators(storage);
            readCasts(storage);

            connection.commit();
            finishLoaders();
            LOG.info(Messages.JdbcLoader_log_succes_queried);
        } catch (InterruptedException ex) {
            throw ex;
        } catch (Exception e) {
            // connection is closed at this point, trust Postgres to rollback it; we're a read-only xact anyway
            throw new IOException(Messages.Connection_DatabaseJdbcAccessError.formatted(getCurrentLocation(),
                    e.getLocalizedMessage()), e);
        }

        return storage;
    }

    private void readFunctions(MetaStorage storage)
            throws InterruptedException, SQLException {
        try (ResultSet result = getStatement().executeQuery(QUERY_SYSTEM_FUNCTIONS)) {
            while (result.next()) {
                IMonitor.checkCancelled(getMonitor());
                String functionName = result.getString(NAME);
                String schemaName = result.getString(NAMESPACE_NAME);

                String arguments = result.getString("proarguments");
                IPgJdbcReader.checkObjectValidity(arguments, DbObjType.FUNCTION, functionName);
                String signature = PgDiffUtils.getQuotedName(functionName) + '(' + arguments + ')';

                MetaFunction function = new MetaFunction(schemaName, signature, functionName);

                String[] arr = PgJdbcUtils.getColArray(result, "proargmodes", true);
                if (arr != null) {
                    List<String> argModes = Arrays.asList(arr);
                    if (argModes.contains("t")) {
                        Long[] argTypeOids = PgJdbcUtils.getColArray(result, "proallargtypes");
                        String[] argNames = PgJdbcUtils.getColArray(result, "proargnames");

                        IntStream.range(0, argModes.size())
                                .filter(i -> "t".equals(argModes.get(i)))
                                .forEach(e -> {
                                    PgJdbcType returnType = getCachedTypeByOid(argTypeOids[e]);
                                    function.addReturnsColumn(argNames[e], returnType.getFullName(schemaName));
                                });
                    }
                }
                if (function.getReturnsColumns().isEmpty()) {
                    String prorettype = result.getString("prorettype");
                    IPgJdbcReader.checkTypeValidity(prorettype);
                    function.setReturns(prorettype);
                }

                function.setSetof(result.getBoolean("proretset"));


                if (!arguments.isEmpty()) {
                    submitAntlrTask('(' + arguments + ')',
                            p -> p.function_args_parser().function_args(),
                            ctx -> {
                                fillArguments(ctx, function);
                                storage.addMetaChild(function);
                            });
                } else {
                    storage.addMetaChild(function);
                }
            }
        }
    }

    private void fillArguments(Function_argsContext ctx, MetaFunction func) {
        for (Function_argumentsContext argument : ctx.function_arguments()) {
            func.addArgument(getArgument(argument));
        }
        if (ctx.agg_order() != null) {
            for (Function_argumentsContext argument : ctx.agg_order().function_arguments()) {
                func.addOrderBy(getArgument(argument));
            }
        }
    }

    private Argument getArgument(Function_argumentsContext argument) {
        Identifier_nontypeContext name = argument.identifier_nontype();
        Argument arg = new Argument(ParserAbstract.parseArgMode(argument.argmode()),
                (name != null ? name.getText() : null),
                ParserAbstract.getFullCtxText(argument.data_type()));

        VexContext def = argument.vex();
        if (def != null) {
            arg.setDefaultExpression(ParserAbstract.getFullCtxText(def));
        }

        return arg;
    }

    private void readRelations(MetaStorage storage)
            throws InterruptedException, SQLException {
        try (ResultSet result = getStatement().executeQuery(QUERY_SYSTEM_RELATIONS)) {
            while (result.next()) {
                IMonitor.checkCancelled(getMonitor());
                String schemaName = result.getString(NAMESPACE_NAME);
                String relationName = result.getString(NAME);

                DbObjType type = switch (result.getString("relkind")) {
                    case "v", "m" -> DbObjType.VIEW;
                    case "S" -> DbObjType.SEQUENCE;
                    default -> DbObjType.TABLE;
                };
                MetaRelation relation = new MetaRelation(schemaName, relationName, type);

                String[] colNames = PgJdbcUtils.getColArray(result, "col_names", true);
                if (colNames != null) {
                    String[] colTypes = PgJdbcUtils.getColArray(result, "col_types");
                    List<Pair<String, String>> columns = new ArrayList<>(colNames.length);
                    for (int i = 0; i < colNames.length; i++) {
                        IPgJdbcReader.checkTypeValidity(colTypes[i]);
                        columns.add(new Pair<>(colNames[i], colTypes[i]));
                    }

                    relation.addColumns(columns);
                }

                storage.addMetaChild(relation);
            }
        }
    }

    private void readOperators(MetaStorage storage) throws InterruptedException, SQLException {
        try (ResultSet result = getStatement().executeQuery(QUERY_SYSTEM_OPERATORS)) {
            while (result.next()) {
                IMonitor.checkCancelled(getMonitor());
                String name = result.getString(NAME);
                String schemaName = result.getString(NAMESPACE_NAME);
                long leftType = result.getLong("left");
                long rightType = result.getLong("right");
                String left = null;
                String right = null;
                if (leftType > 0) {
                    left = getCachedTypeByOid(leftType).getFullName(schemaName);
                }
                if (rightType > 0) {
                    right = getCachedTypeByOid(rightType).getFullName(schemaName);
                }

                MetaOperator operator = new MetaOperator(schemaName, name);
                operator.setLeftArg(left);
                operator.setRightArg(right);
                operator.setReturns(getCachedTypeByOid(result.getLong("result")).getFullName(schemaName));

                storage.addMetaChild(operator);
            }
        }
    }

    private void readCasts(MetaStorage storage) throws InterruptedException, SQLException {
        try (PreparedStatement statement = getConnection().prepareStatement(QUERY_SYSTEM_CASTS)) {
            statement.setLong(1, getLastSysOid());
            ResultSet result = getRunner().runScript(statement);
            while (result.next()) {
                IMonitor.checkCancelled(getMonitor());
                String source = result.getString("source");
                IPgJdbcReader.checkTypeValidity(source);
                String target = result.getString("target");
                IPgJdbcReader.checkTypeValidity(target);
                String type = result.getString("castcontext");
                CastContext ctx = switch (type) {
                    case "e" -> CastContext.EXPLICIT;
                    case "a" -> CastContext.ASSIGNMENT;
                    case "i" -> CastContext.IMPLICIT;
                    default -> throw new IllegalStateException("Unknown cast context: " + type);
                };
                storage.addMetaChild(new MetaCast(source, target, ctx));
            }
        }
    }

    /**
     * Serializes system objects from a database connection to a file.
     *
     * @param path the output file path
     * @param url  the database connection URL
     * @throws IOException          if an I/O error occurs
     * @throws InterruptedException if the operation is interrupted
     */
    public static void serialize(String path, String url) throws IOException, InterruptedException {
        var jdbcConnector = new PgJdbcConnector(url);
        Serializable storage = new PgJdbcSystemLoader(jdbcConnector).getStorageFromJdbc();
        Utils.serialize(path, storage);
    }
}