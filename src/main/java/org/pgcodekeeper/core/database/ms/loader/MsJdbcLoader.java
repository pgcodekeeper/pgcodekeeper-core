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
package org.pgcodekeeper.core.database.ms.loader;

import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.function.*;

import org.pgcodekeeper.core.database.api.jdbc.IJdbcConnector;
import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.jdbc.QueryBuilder;
import org.pgcodekeeper.core.database.base.loader.AbstractJdbcLoader;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.database.ms.MsDiffUtils;
import org.pgcodekeeper.core.database.ms.jdbc.*;
import org.pgcodekeeper.core.database.ms.parser.MsParserUtils;
import org.pgcodekeeper.core.database.ms.parser.generated.TSQLParser;
import org.pgcodekeeper.core.database.ms.schema.*;
import org.pgcodekeeper.core.ignorelist.IgnoreSchemaList;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.settings.ISettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC-based database schema loader for Microsoft SQL Server databases.
 * Reads database schemas, tables, functions, procedures, views, types, assemblies, roles, users and other objects from a Microsoft SQL Server database.
 * Extends JdbcLoaderBase to provide Microsoft SQL Server-specific loading functionality.
 */
public final class MsJdbcLoader extends AbstractJdbcLoader<MsDatabase> {

    private static final Logger LOG = LoggerFactory.getLogger(MsJdbcLoader.class);

    private static final String QUERY_CHECK_MS_VERSION = new QueryBuilder()
            .column("CAST(LEFT(CAST(SERVERPROPERTY('productversion') AS varchar), 2) AS INT)")
            .build();

    /**
     * Creates a new Microsoft SQL Server JDBC loader with the specified parameters.
     *
     * @param connector        the JDBC connector for establishing database connections
     * @param settings         loader settings and configuration
     * @param monitor          progress monitor for tracking loading progress
     * @param ignoreSchemaList list of schemas to ignore during loading
     */
    public MsJdbcLoader(IJdbcConnector connector, ISettings settings,
                        IMonitor monitor, IgnoreSchemaList ignoreSchemaList) {
        super(connector, monitor, settings, ignoreSchemaList);
    }

    @Override
    public MsDatabase load() throws IOException, InterruptedException {
        MsDatabase d = createDatabase();

        LOG.info(Messages.JdbcLoader_log_reading_db_jdbc);
        setCurrentOperation(Messages.JdbcChLoader_log_connection_db);
        try (Connection connection = connector.getConnection();
             Statement statement = connection.createStatement()) {
            this.connection = connection;
            this.statement = statement;

            connection.setAutoCommit(false);
            // TODO maybe not needed and/or may cause extra locking (compared to PG)
            // may need to be removed, Source Control seems to work in default READ COMMITTED state
            getRunner().run(statement, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");

            // TODO add counting objects later
//            setupMonitorWork();

            queryCheckMsVersion();

            LOG.info(Messages.JdbcLoader_log_read_db_objects);
            new MsSchemasReader(this, d).read();
            new MsTablesReader(this).read();
            new MsFPVTReader(this).read();
            new MsExtendedObjectsReader(this).read();
            new MsSequencesReader(this).read();
            new MsIndicesAndPKReader(this).read();
            new MsFKReader(this).read();
            new MsCheckConstraintsReader(this).read();
            new MsTypesReader(this).read();
            new MsAssembliesReader(this, d).read();
            new MsRolesReader(this, d).read();
            new MsUsersReader(this, d).read();
            new MsStatisticsReader(this).read();

            finishLoaders();

            connection.commit();

            LOG.info(Messages.JdbcLoader_log_succes_queried);
        } catch (InterruptedException ex) {
            throw ex;
        } catch (Exception e) {
            // connection is closed at this point
            throw new IOException(Messages.Connection_DatabaseJdbcAccessError.formatted(getCurrentLocation(),
                    e.getLocalizedMessage()), e);
        }
        return d;
    }

    private void queryCheckMsVersion() throws SQLException, InterruptedException {
        setCurrentOperation(Messages.JdbcLoaderBase_log_reading_ms_version);
        try (ResultSet res = runner.runScript(statement, QUERY_CHECK_MS_VERSION)) {
            version = res.next() ? res.getInt(1) : MsSupportedVersion.VERSION_17.getVersion();
            if (!MsSupportedVersion.VERSION_17.isLE(version)) {
                throw new IllegalStateException(Messages.JdbcLoaderBase_unsupported_ms_sql_version);
            }

            debug(Messages.JdbcLoaderBase_log_load_version, version);
        }
    }

    public <T> void submitMsAntlrTask(String sql, Function<TSQLParser, T> parserCtxReader, Consumer<T> finalizer) {
        BiFunction<List<Object>, String, TSQLParser> createFunction =
                (list, location) -> MsParserUtils.createSqlParser(sql, location, list);
        submitAntlrTask(createFunction, parserCtxReader, finalizer);
    }

    public static String getMsType(AbstractStatement statement, String schema, String dataType,
                                   boolean isUserDefined, int size, int precision, int scale) {
        return getMsType(statement, schema, dataType, isUserDefined, size, precision, scale, true);
    }

    public static String getMsType(AbstractStatement statement, String schema, String dataType,
                                   boolean isUserDefined, int size, int precision, int scale, boolean quoteSysTypes) {
        StringBuilder sb = new StringBuilder();

        if (isUserDefined) {
            statement.addDependency(new ObjectReference(schema, dataType, DbObjType.TYPE));
            sb.append(MsDiffUtils.quoteName(schema)).append('.');
        }

        boolean quoteName = isUserDefined || quoteSysTypes || !MsDiffUtils.isSystemSchema(schema);
        sb.append(quoteName ? MsDiffUtils.quoteName(dataType) : dataType);

        switch (dataType) {
            case "varbinary", "nvarchar", "varchar", "nchar", "char", "binary":
                if (size == -1) {
                    sb.append(" (max)");
                } else {
                    sb.append(" (").append(size).append(')');
                }
                break;
            case "datetime2", "datetimeoffset", "time":
                sb.append(" (").append(scale).append(')');
                break;
            case "decimal", "numeric":
                sb.append(" (").append(precision).append(", ").append(scale).append(')');
                break;
            default:
                break;
        }

        return sb.toString();
    }

    public void setPrivileges(AbstractStatement st, List<MsXmlReader> privs) {
        if (settings.isIgnorePrivileges()) {
            return;
        }

        for (MsXmlReader acl : privs) {
            String state = acl.getString("sd");
            boolean isWithGrantOption = false;
            if ("GRANT_WITH_GRANT_OPTION".equals(state)) {
                state = "GRANT";
                isWithGrantOption = true;
            }

            String permission = acl.getString("pn");
            String role = acl.getString("r");
            String col = null;
            StringBuilder sb = new StringBuilder();

            if (st instanceof ISearchPath) {
                col = acl.getString("c");
                if (st.getStatementType() == DbObjType.TYPE) {
                    sb.append("TYPE::");
                }

                sb.append(st.getQualifiedName());

                if (col != null) {
                    sb.append('(').append(MsDiffUtils.quoteName(col)).append(')');
                }
            } else {
                sb.append(st.getStatementType()).append("::").append(MsDiffUtils.quoteName(st.getName()));
            }

            IPrivilege priv = new MsPrivilege(state, permission, sb.toString(),
                    MsDiffUtils.quoteName(role), isWithGrantOption);

            if (col != null && st instanceof MsTable table) {
                table.getColumn(col).addPrivilege(priv);
            } else {
                st.addPrivilege(priv);
            }
        }
    }

    @Override
    protected MsDatabase createDatabase() {
        return new MsDatabase();
    }
}