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
package org.pgcodekeeper.core.loader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import org.eclipse.core.runtime.SubMonitor;
import org.pgcodekeeper.core.PgDiffUtils;
import org.pgcodekeeper.core.loader.jdbc.JdbcLoaderBase;
import org.pgcodekeeper.core.loader.jdbc.JdbcReader;
import org.pgcodekeeper.core.loader.jdbc.JdbcType;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.Function_argsContext;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.Function_argumentsContext;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.Identifier_nontypeContext;
import org.pgcodekeeper.core.parsers.antlr.generated.SQLParser.VexContext;
import org.pgcodekeeper.core.parsers.antlr.statements.ParserAbstract;
import org.pgcodekeeper.core.schema.AbstractDatabase;
import org.pgcodekeeper.core.schema.Argument;
import org.pgcodekeeper.core.schema.ICast.CastContext;
import org.pgcodekeeper.core.schema.meta.MetaCast;
import org.pgcodekeeper.core.schema.meta.MetaFunction;
import org.pgcodekeeper.core.schema.meta.MetaOperator;
import org.pgcodekeeper.core.schema.meta.MetaRelation;
import org.pgcodekeeper.core.schema.meta.MetaStorage;
import org.pgcodekeeper.core.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JdbcSystemLoader extends JdbcLoaderBase {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcSystemLoader.class);

    private static final String NAMESPACE_NAME = "nspname";
    private static final String NAME = "name";

    private final String timezone;

    public JdbcSystemLoader(AbstractJdbcConnector connector, String timezone, SubMonitor monitor) {
        super(connector, monitor, null, null);
        this.timezone = timezone;
    }

    @Override
    public AbstractDatabase load() throws IOException, InterruptedException {
        throw new IllegalStateException("Unsupported operation for JdbcSystemLoader");
    }

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
            getRunner().run(statement, "SET timezone = " + PgDiffUtils.quoteString(timezone));

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
            throw new IOException(MessageFormat.format(Messages.Connection_DatabaseJdbcAccessError,
                    e.getLocalizedMessage(), getCurrentLocation()), e);
        }

        return storage;
    }

    private void readFunctions(MetaStorage storage)
            throws InterruptedException, SQLException {
        try (ResultSet result = getStatement().executeQuery(JdbcQueries.QUERY_SYSTEM_FUNCTIONS)) {
            while (result.next()) {
                PgDiffUtils.checkCancelled(getMonitor());
                String functionName = result.getString(NAME);
                String schemaName = result.getString(NAMESPACE_NAME);

                String arguments = result.getString("proarguments");
                JdbcReader.checkObjectValidity(arguments, DbObjType.FUNCTION, functionName);
                String signature = PgDiffUtils.getQuotedName(functionName) + '(' + arguments + ')';

                MetaFunction function = new MetaFunction(schemaName, signature, functionName);

                String[] arr = JdbcReader.getColArray(result, "proargmodes");
                if (arr != null) {
                    List<String> argModes = Arrays.asList(arr);
                    if (argModes.contains("t")) {
                        Long[] argTypeOids = JdbcReader.getColArray(result, "proallargtypes");
                        String[] argNames = JdbcReader.getColArray(result, "proargnames");

                        IntStream.range(0, argModes.size())
                        .filter(i -> "t".equals(argModes.get(i)))
                        .forEach(e -> {
                            JdbcType returnType = getCachedTypeByOid(argTypeOids[e]);
                            function.addReturnsColumn(argNames[e], returnType.getFullName(schemaName));
                        });
                    }
                }
                if (function.getReturnsColumns().isEmpty()) {
                    String prorettype = result.getString("prorettype");
                    JdbcReader.checkTypeValidity(prorettype);
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
        try (ResultSet result = getStatement().executeQuery(JdbcQueries.QUERY_SYSTEM_RELATIONS)) {
            while (result.next()) {
                PgDiffUtils.checkCancelled(getMonitor());
                String schemaName = result.getString(NAMESPACE_NAME);
                String relationName = result.getString(NAME);

                DbObjType type = switch (result.getString("relkind")) {
                case "v", "m" -> DbObjType.VIEW;
                case "S" -> DbObjType.SEQUENCE;
                default -> DbObjType.TABLE;
                };
                MetaRelation relation = new MetaRelation(schemaName, relationName, type);

                String[] colNames = JdbcReader.getColArray(result, "col_names");
                if (colNames != null) {
                    String[] colTypes = JdbcReader.getColArray(result, "col_types");
                    List<Pair<String, String>> columns = new ArrayList<>(colNames.length);
                    for (int i = 0; i < colNames.length; i++) {
                        JdbcReader.checkTypeValidity(colTypes[i]);
                        columns.add(new Pair<>(colNames[i], colTypes[i]));
                    }

                    relation.addColumns(columns);
                }

                storage.addMetaChild(relation);
            }
        }
    }

    private void readOperators(MetaStorage storage) throws InterruptedException, SQLException {
        try (ResultSet result = getStatement().executeQuery(JdbcQueries.QUERY_SYSTEM_OPERATORS)) {
            while (result.next()) {
                PgDiffUtils.checkCancelled(getMonitor());
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
        try (PreparedStatement statement = getConnection().prepareStatement(JdbcQueries.QUERY_SYSTEM_CASTS)) {
            statement.setLong(1, getLastSysOid());
            ResultSet result = getRunner().runScript(statement);
            while (result.next()) {
                PgDiffUtils.checkCancelled(getMonitor());
                String source = result.getString("source");
                JdbcReader.checkTypeValidity(source);
                String target = result.getString("target");
                JdbcReader.checkTypeValidity(target);
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
}