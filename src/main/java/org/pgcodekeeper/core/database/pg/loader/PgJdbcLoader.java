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
package org.pgcodekeeper.core.database.pg.loader;

import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.base.jdbc.QueryBuilder;
import org.pgcodekeeper.core.database.base.schema.AbstractColumn;
import org.pgcodekeeper.core.database.base.schema.AbstractTable;
import org.pgcodekeeper.core.database.pg.PgDiffUtils;
import org.pgcodekeeper.core.database.pg.jdbc.*;
import org.pgcodekeeper.core.database.pg.schema.PgAbstractFunction;
import org.pgcodekeeper.core.database.pg.schema.PgPrivilege;
import org.pgcodekeeper.core.parsers.antlr.base.AntlrUtils;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser;
import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.database.api.jdbc.IJdbcConnector;
import org.pgcodekeeper.core.database.base.loader.AbstractJdbcLoader;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.ignorelist.IgnoreSchemaList;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.database.pg.schema.PgDatabase;
import org.pgcodekeeper.core.parsers.antlr.base.AntlrParser;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.utils.Utils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * JDBC-based database schema loader for PostgreSQL databases.
 * Reads database schemas, functions, views, tables, types, sequences, extensions, and other objects from a PostgreSQL database.
 * Supports timezone configuration and Greenplum database detection.
 * Extends JdbcLoaderBase to provide PostgreSQL-specific loading functionality.
 */
public class PgJdbcLoader extends AbstractJdbcLoader {

    private static final String QUERY_CHECK_GREENPLUM = new QueryBuilder()
            .column("version()")
            .build();

    private static final String QUERY_CHECK_PG_VERSION = new QueryBuilder()
            .column("CAST (pg_catalog.current_setting('server_version_num') AS INT)")
            .build();

    private static final String QUERY_CHECK_USER_PRIVILEGES = new QueryBuilder()
            .column("pg_catalog.has_table_privilege('pg_catalog.pg_user_mapping', 'SELECT') AS result")
            .build();

    private static final String QUERY_CHECK_LAST_SYS_OID = new QueryBuilder()
            .column("datlastsysoid::bigint")
            .from("pg_catalog.pg_database")
            .where("datname = pg_catalog.current_database()")
            .build();

    private static final String QUERY_CHECK_TIMESTAMPS = new QueryBuilder()
            .column("n.nspname")
            .column("e.extversion")
            .column("EXISTS (SELECT 1 FROM pg_catalog.pg_event_trigger WHERE evtenabled != 'O' "
                    + "AND (evtname = 'dbots_tg_on_ddl_event' OR evtname = 'dbots_tg_on_drop_event')) AS disabled")
            .from("pg_catalog.pg_namespace n")
            .join("LEFT JOIN pg_catalog.pg_extension e on e.extnamespace = n.oid")
            .where("e.extname = 'pg_dbo_timestamp'")
            .build();

    private static final String QUERY_TOTAL_OBJECTS_COUNT = new QueryBuilder()
            .column("pg_catalog.count(c.oid)::integer")
            .from("pg_catalog.pg_class c")
            .where("c.relnamespace IN (SELECT nsp.oid FROM pg_catalog.pg_namespace nsp WHERE nsp.nspname NOT LIKE ('pg_%') AND nsp.nspname != 'information_schema')")
            .where("c.relkind != 't'")
            .build();

    private static final String QUERY_TYPES_FOR_CACHE_ALL = new QueryBuilder()
            .column("t.oid")
            .column("t.typname")
            .column("t.typelem")
            .column("t.typarray")
            .column("te.typname AS elemname")
            .column("n.nspname")
            .from("pg_catalog.pg_type t")
            .join("LEFT JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid")
            .join("LEFT JOIN pg_catalog.pg_type te ON te.oid = t.typelem")
            .build();

    /**
     * OID of the first user object
     *
     * @see <a href="https://github.com/postgres/postgres/blob/master/src/include/access/transam.h">transam.h</a>
     */
    private static final int FIRST_NORMAL_OBJECT_ID = 16384;
    private static final int DEFAULT_OBJECTS_COUNT = 100;

    protected final String timezone;

    private String extensionSchema;
    private boolean isGreenplumDb;
    private long lastSysOid;
    private Map<Long, PgJdbcType> cachedTypesByOid;

    /**
     * Creates a new PostgreSQL JDBC loader with the specified parameters.
     *
     * @param connector        the JDBC connector for establishing database connections
     * @param timezone         the timezone to set for the database connection
     * @param settings         loader settings and configuration
     * @param monitor          progress monitor for tracking loading progress
     */
    public PgJdbcLoader(IJdbcConnector connector, String timezone, ISettings settings,
                        IMonitor monitor, IgnoreSchemaList ignoreSchemaList) {
        super(connector, monitor, settings, ignoreSchemaList);
        this.timezone = timezone;
    }

    @Override
    public PgDatabase load() throws IOException, InterruptedException {
        PgDatabase d = new PgDatabase();

        info(Messages.JdbcLoader_log_reading_db_jdbc);
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
            queryRoles();
            queryCheckExtension();
            setupMonitorWork();

            info(Messages.JdbcLoader_log_read_db_objects);
            new PgSchemasReader(this, d).read();

            // NOTE: order of readers has been changed to move the heaviest ANTLR tasks to the beginning
            // to give them a chance to finish while JDBC processes other non-ANTLR stuff
            new PgFunctionsReader(this).read();
            new PgViewsReader(this).read();
            new PgTablesReader(this).read();
            new PgRulesReader(this).read();
            if (SupportedPgVersion.GP_VERSION_7.isLE(getVersion())) {
                new PgPoliciesReader(this).read();
            }
            new PgTriggersReader(this).read();
            new PgIndicesReader(this).read();
            new PgConstraintsReader(this).read();
            new PgTypesReader(this).read();
            if (SupportedPgVersion.GP_VERSION_7.isLE(getVersion())) {
                new PgStatisticsReader(this).read();
            }

            // non-ANTLR tasks
            var sequencesReader = new PgSequencesReader(this);
            sequencesReader.read();
            new PgFtsParsersReader(this).read();
            new PgFtsTemplatesReader(this).read();
            new PgFtsDictionariesReader(this).read();
            new PgFtsConfigurationsReader(this).read();
            new PgOperatorsReader(this).read();

            new PgExtensionsReader(this, d).read();
            new PgEventTriggersReader(this, d).read();
            new PgCastsReader(this, d).read();
            new PgForeignDataWrappersReader(this, d).read();
            new PgServersReader(this, d).read();
            try (ResultSet res = getRunner().runScript(statement, QUERY_CHECK_USER_PRIVILEGES)) {
                if (res.next() && res.getBoolean("result")) {
                    new PgUserMappingsReader(this, d).read();
                }
            }
            new PgCollationsReader(this).read();

            if (!SupportedPgVersion.GP_VERSION_7.isLE(getVersion())) {
                sequencesReader.querySequencesData(d);
            }
            connection.commit();
            finishLoaders();

            d.sortColumns();

            d.setVersion(SupportedPgVersion.valueOf(getVersion()));
            info(Messages.JdbcLoader_log_succes_queried);
        } catch (InterruptedException ex) {
            throw ex;
        } catch (Exception e) {
            // connection is closed at this point, trust Postgres to rollback it; we're a read-only xact anyway
            throw new IOException(Messages.Connection_DatabaseJdbcAccessError.formatted(getCurrentLocation(),
                    e.getLocalizedMessage()), e);
        }
        return d;
    }

    protected void queryCheckGreenplumDb() throws SQLException, InterruptedException {
        setCurrentOperation(Messages.JdbcLoaderBase_log_check_gp_db);
        try (ResultSet res = getRunner().runScript(statement, QUERY_CHECK_GREENPLUM)) {
            if (res.next()) {
                isGreenplumDb = res.getString(1).contains(Consts.GREENPLUM);
            }
        }
        debug(Messages.JdbcLoaderBase_log_get_result_gp, isGreenplumDb);
    }

    protected void queryCheckPgVersion() throws SQLException, InterruptedException {
        setCurrentOperation(Messages.JdbcLoaderBase_log_reading_pg_version);
        int version;
        try (ResultSet res = runner.runScript(statement, QUERY_CHECK_PG_VERSION)) {
            version = res.next() ? res.getInt(1) : SupportedPgVersion.GP_VERSION_6.getVersion();
            setVersion(version);
            debug(Messages.JdbcLoaderBase_log_load_version, getVersion());
        }
        if (!isGreenplumDb && !SupportedPgVersion.VERSION_14.isLE(version)) {
            throw new IllegalStateException(Messages.JdbcLoaderBase_unsupported_pg_version);
        }
        if (isGreenplumDb && !SupportedPgVersion.GP_VERSION_6.isLE(version)) {
            throw new IllegalStateException(Messages.JdbcLoaderBase_unsupported_gp_version);
        }
    }

    protected void queryCheckLastSysOid() throws SQLException, InterruptedException {
        setCurrentOperation(Messages.JdbcLoaderBase_log_get_last_oid);
        if (SupportedPgVersion.VERSION_15.isLE(getVersion())) {
            lastSysOid = FIRST_NORMAL_OBJECT_ID - 1L;
        } else {
            try (ResultSet res = runner.runScript(statement, QUERY_CHECK_LAST_SYS_OID)) {
                lastSysOid = res.next() ? res.getLong(1) : 10_000;
            }
        }
        debug(Messages.JdbcLoaderBase_log_get_last_system_obj_oid, lastSysOid);
    }

    protected void queryTypesForCache() throws SQLException, InterruptedException {
        cachedTypesByOid = new HashMap<>();
        setCurrentOperation(Messages.JdbcLoaderBase_log_get_list_system_types);
        try (ResultSet res = runner.runScript(statement, QUERY_TYPES_FOR_CACHE_ALL)) {
            while (res.next()) {
                long oid = res.getLong("oid");
                PgJdbcType type = new PgJdbcType(oid, res.getString("typname"),
                        res.getLong("typelem"), res.getLong("typarray"),
                        res.getString("nspname"), res.getString("elemname"), lastSysOid);
                cachedTypesByOid.put(oid, type);
            }
        }
    }

    protected void queryRoles() throws SQLException, InterruptedException {
        if (settings.isIgnorePrivileges()) {
            return;
        }
        cachedRolesNamesByOid = new HashMap<>();
        setCurrentOperation(Messages.JdbcLoaderBase_log_get_roles);
        try (ResultSet res = runner.runScript(statement, "SELECT oid::bigint, rolname FROM pg_catalog.pg_roles")) {
            while (res.next()) {
                cachedRolesNamesByOid.put(res.getLong("oid"), res.getString("rolname"));
            }
        }
    }

    protected void queryCheckExtension() throws SQLException, InterruptedException {
        setCurrentOperation(Messages.JdbcLoaderBase_log_check_extension);
        try (ResultSet res = runner.runScript(statement, QUERY_CHECK_TIMESTAMPS)) {
            while (res.next()) {
                String extVersion = res.getString("extversion");
                if (!extVersion.startsWith(Consts.EXTENSION_VERSION)) {
                    var msg = Messages.JdbcLoaderBase_log_old_version_used.formatted(extVersion,
                            Consts.EXTENSION_VERSION);
                    info(msg);
                } else if (res.getBoolean("disabled")) {
                    info(Messages.JdbcLoaderBase_log_event_trigger_disabled);
                } else {
                    extensionSchema = res.getString("nspname");
                }
            }
        }
    }

    protected void setupMonitorWork() throws SQLException, InterruptedException {
        setCurrentOperation(Messages.JdbcLoaderBase_log_get_obj_count);
        try (ResultSet resCount = runner.runScript(statement, QUERY_TOTAL_OBJECTS_COUNT)) {
            int count = resCount.next() ? resCount.getInt(1) : DEFAULT_OBJECTS_COUNT;
            monitor.setWorkRemaining(count);
            debug(Messages.JdbcLoaderBase_log_get_total_obj_count, count);
        }
    }

    public <T> void submitAntlrTask(String sql, Function<SQLParser, T> parserCtxReader, Consumer<T> finalizer) {
        BiFunction<List<Object>, String, SQLParser> createFunction =
                (list, location) -> AntlrParser.createSQLParser(sql, location, list);
        submitAntlrTask(createFunction, parserCtxReader, finalizer);
    }

    public <T> void submitPlpgsqlTask(String sql, Function<SQLParser, T> parserCtxReader, Consumer<T> finalizer) {
        BiFunction<List<Object>, String, SQLParser> createFunction = (list, location) -> {
            var parser = AntlrParser.createSQLParser(sql, location, list);
            AntlrUtils.removeIntoStatements(parser);
            return parser;
        };

        submitAntlrTask(createFunction, parserCtxReader, finalizer);
    }

    public void setPrivileges(AbstractStatement st, String aclItemsArrayAsString, String schemaName) {
        setPrivileges(st, aclItemsArrayAsString, null, schemaName);
    }

    public void setPrivileges(AbstractColumn column, AbstractTable t, String aclItemsArrayAsString, String schemaName) {
        setPrivileges(column, PgDiffUtils.getQuotedName(t.getName()), aclItemsArrayAsString,
                t.getOwner(), PgDiffUtils.getQuotedName(column.getName()), schemaName);
    }

    public void setPrivileges(AbstractStatement st, String aclItemsArrayAsString, String columnName, String schemaName) {
        DbObjType type = st.getStatementType();
        String signature;
        if (type.in(DbObjType.FUNCTION, DbObjType.PROCEDURE, DbObjType.AGGREGATE)) {
            signature = ((PgAbstractFunction) st).appendFunctionSignature(new StringBuilder(), false, true).toString();
        } else {
            signature = PgDiffUtils.getQuotedName(st.getName());
        }

        String owner = st.getOwner();
        if (owner == null && type == DbObjType.SCHEMA && Consts.PUBLIC.equals(st.getName())) {
            owner = "postgres";
        }

        setPrivileges(st, signature, aclItemsArrayAsString, owner,
                columnName == null ? null : PgDiffUtils.getQuotedName(columnName), schemaName);
    }

    public void setOwner(AbstractStatement statement, long ownerOid) {
        if (!settings.isIgnorePrivileges()) {
            statement.setOwner(getRoleByOid(ownerOid));
        }
    }

    /**
     * Parses <code>aclItemsArrayAsString</code> and adds parsed privileges to
     * <code>PgStatement</code> object. Owner privileges go first.
     * <br>
     * Currently supports privileges only on PgSequence, PgTable, PgView, PgColumn,
     * PgFunction, PgSchema, PgType, PgDomain
     *
     * @param st                    PgStatement object where privileges to be added
     * @param stSignature           PgStatement signature (differs in different PgStatement instances)
     * @param aclItemsArrayAsString Input acl string in the
     *                              form of "{grantee=grant_chars/grantor[, ...]}"
     * @param owner                 the owner of PgStatement object (why separate?)
     * @param columnId              column name, if this aclItemsArrayAsString is column
     *                              privilege string; otherwise null
     * @param schemaName            name of schema for 'PgStatement st'
     */
    /*
     * See parseAclItem() in dumputils.c
     * For privilege characters see JdbcAclParser.PrivilegeTypes
     * Order of all characters (for all types of objects combined) : raxdtDXCcTUw
     */
    private void setPrivileges(AbstractStatement st, String stSignature,
                               String aclItemsArrayAsString, String owner, String columnId, String schemaName) {
        if (aclItemsArrayAsString == null || settings.isIgnorePrivileges()) {
            return;
        }
        DbObjType type = st.getStatementType();
        String stType = null;
        boolean isFunctionOrTypeOrDomain = false;
        String order;
        switch (type) {
            case SEQUENCE:
                order = "rUw";
                break;

            case TABLE, VIEW, COLUMN:
                stType = "TABLE";
                if (columnId != null) {
                    order = "raxw";
                } else if (SupportedPgVersion.VERSION_17.isLE(version)) {
                    order = "raxdtDwm";
                } else {
                    order = "raxdtDw";
                }
                break;

            case AGGREGATE:
                // For grant permissions to AGGREGATE in postgres used operator 'FUNCTION'.
                // For example grant permissions to AGGREGATE public.mode(boolean):
                // GRANT ALL ON FUNCTION public.mode(boolean) TO test_user;
                stType = "FUNCTION";

                // For grant permissions to AGGREGATE without arguments as signature
                // used only left and right paren.
                if (stSignature.contains("*")) {
                    stSignature = stSignature.replace("*", "");
                }
                // $FALL-THROUGH$
            case FUNCTION, PROCEDURE:
                order = "X";
                isFunctionOrTypeOrDomain = true;
                break;

            case SCHEMA:
                order = "CU";
                break;

            case TYPE, DOMAIN:
                stType = "TYPE";
                order = "U";
                isFunctionOrTypeOrDomain = true;
                break;
            case SERVER:
                stType = "FOREIGN SERVER";
                order = "U";
                break;
            case FOREIGN_DATA_WRAPPER:
                stType = "FOREIGN DATA WRAPPER";
                order = "U";
                break;
            default:
                throw new IllegalStateException(type + Messages.JdbcLoaderBase_log_not_support_privil);
        }
        if (stType == null) {
            stType = st.getStatementType().name();
        }

        String qualStSignature = schemaName == null ? stSignature
                : PgDiffUtils.getQuotedName(schemaName) + '.' + stSignature;
        String column = columnId != null ? "(" + columnId + ")" : "";

        List<PgJdbcPrivilege> grants = PgJdbcPrivilege.parse(aclItemsArrayAsString, order, owner);

        boolean metPublicRoleGrants = false;
        boolean metDefaultOwnersGrants = false;
        for (PgJdbcPrivilege p : grants) {
            if (p.isGrantAllToPublic()) {
                metPublicRoleGrants = true;
            }
            if (p.isDefault()) {
                metDefaultOwnersGrants = true;
            }
        }

        // FUNCTION/TYPE/DOMAIN by default has "GRANT ALL to PUBLIC".
        // If "GRANT ALL to PUBLIC" for FUNCTION/TYPE/DOMAIN is absent, then
        // in this case for them explicitly added "REVOKE ALL from PUBLIC".
        if (!metPublicRoleGrants && isFunctionOrTypeOrDomain) {
            st.addPrivilege(new PgPrivilege("REVOKE", "ALL" + column,
                    stType + " " + qualStSignature, "PUBLIC", false));
        }

        // 'REVOKE ALL' for COLUMN never happened, because of the overlapping
        // privileges from the table.
        if (column.isEmpty() && !metDefaultOwnersGrants) {
            st.addPrivilege(new PgPrivilege("REVOKE", "ALL" + column,
                    stType + " " + qualStSignature, PgDiffUtils.getQuotedName(owner), false));
        }

        for (PgJdbcPrivilege grant : grants) {
            // Always add if statement type is COLUMN, because of the specific
            // relationship with table privileges.
            // The privileges of columns for role are not set lower than for the
            // same role in the parent table, they may be the same or higher.
            //
            // Skip if default owner's privileges
            // or if it is 'GRANT ALL ON FUNCTION/TYPE/DOMAIN schema.name TO PUBLIC'
            if (column.isEmpty() && (grant.isDefault() ||
                    (isFunctionOrTypeOrDomain && grant.isGrantAllToPublic()))) {
                continue;
            }
            st.addPrivilege(new PgPrivilege("GRANT", grant.getGrantString(column),
                    stType + " " + qualStSignature, grant.getGrantee(), grant.isGO()));
        }
    }

    public void setAuthor(AbstractStatement st, ResultSet res) throws SQLException {
        if (extensionSchema != null) {
            st.setAuthor(res.getString("ses_user"));
        }
    }

    public long getLastSysOid() {
        return lastSysOid;
    }

    public String getExtensionSchema() {
        return extensionSchema;
    }

    public boolean isGreenplumDb() {
        return isGreenplumDb;
    }

    public PgJdbcType getCachedTypeByOid(Long oid) {
        return cachedTypesByOid.get(oid);
    }

    public String getRoleByOid(long oid) {
        if (settings.isIgnorePrivileges()) {
            return null;
        }
        return oid == 0 ? "PUBLIC" : cachedRolesNamesByOid.get(oid);
    }
}
