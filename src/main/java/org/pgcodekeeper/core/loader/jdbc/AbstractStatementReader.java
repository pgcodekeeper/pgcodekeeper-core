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
package org.pgcodekeeper.core.loader.jdbc;

import org.pgcodekeeper.core.database.pg.PgDiffUtils;
import org.pgcodekeeper.core.loader.QueryBuilder;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.monitor.IMonitor;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Abstract base class for JDBC statement readers that process database metadata.
 * Provides common functionality for building SQL queries with extension and description support,
 * and processing database objects from ResultSets.
 */
public abstract class AbstractStatementReader {

    private static final String PG_CATALOG = "pg_catalog.";

    private static final QueryBuilder MS_PRIVILEGES_JOIN_SUBSELECT = new QueryBuilder()
            .column("perm.state_desc AS sd")
            .column("perm.permission_name AS pn")
            .column("roleprinc.name AS r")
            .from("sys.database_principals roleprinc WITH (NOLOCK)")
            .join("JOIN sys.database_permissions perm WITH (NOLOCK) ON perm.grantee_principal_id = roleprinc.principal_id")
            .join("LEFT JOIN sys.columns col WITH (NOLOCK) ON col.object_id = perm.major_id AND col.column_id = perm.minor_id")
            .postAction("FOR XML RAW, ROOT");

    protected static final QueryBuilder EXTENSION_DEPS_CTE_BUILDER = new QueryBuilder()
            .column("objid")
            .from("pg_catalog.pg_depend");

    // join extension data with a left join
    private static final String EXTENSION_JOIN =
            "LEFT JOIN %s.dbots_event_data time ON time.objid = res.oid AND time.classid = %s::pg_catalog.regclass";

    // join description data with a left join
    private static final String DESCRIPTION_JOIN =
            "LEFT JOIN pg_catalog.pg_description d ON d.objoid = res.oid AND d.classoid = %s::pg_catalog.regclass %s";

    protected final JdbcLoaderBase loader;
    private final String classId;

    protected AbstractStatementReader(JdbcLoaderBase loader) {
        this.loader = loader;
        String tmpClassId = getClassId();
        this.classId = tmpClassId == null ? null : PgDiffUtils.quoteString(PG_CATALOG + tmpClassId);
    }

    /**
     * Reads database objects by executing the generated SQL query and processing results.
     *
     * @throws SQLException         if database access fails
     * @throws InterruptedException if reading is interrupted
     * @throws XmlReaderException   if XML processing fails
     */
    public final void read() throws SQLException, InterruptedException, XmlReaderException {
        loader.setCurrentOperation(Messages.AbstractStatementReader_start + getClass().getSimpleName());
        QueryBuilder builder = makeQuery();
        if (builder == null) {
            return;
        }
        String query = builder.build();

        try (PreparedStatement statement = loader.getConnection().prepareStatement(query)) {
            setParams(statement);
            ResultSet result = loader.getRunner().runScript(statement);
            while (result.next()) {
                IMonitor.checkCancelled(loader.getMonitor());
                loader.getMonitor().worked(1);
                processResult(result);
            }
        }
    }

    protected QueryBuilder makeQuery() {
        QueryBuilder builder = new QueryBuilder();
        fillQueryBuilder(builder);
        appendExtension(builder);
        return builder;
    }

    private void appendExtension(QueryBuilder builder) {
        String extensionSchema = loader.getExtensionSchema();
        if (extensionSchema != null) {
            builder.column("time.ses_user");
            builder.join(EXTENSION_JOIN.formatted(PgDiffUtils.getQuotedName(extensionSchema), classId));
        }
    }

    protected void addExtensionDepsCte(QueryBuilder builder) {
        QueryBuilder subSelect = getExtensionCte()
                .where("classid = %s::pg_catalog.regclass".formatted(classId));
        builder.with("extension_deps", subSelect);
        builder.where("res.oid NOT IN (SELECT objid FROM extension_deps)");
    }

    protected QueryBuilder getExtensionCte() {
        return EXTENSION_DEPS_CTE_BUILDER.copy().where("deptype = 'e'");
    }

    protected void addDescriptionPart(QueryBuilder builder) {
        addDescriptionPart(builder, false);
    }

    protected void addDescriptionPart(QueryBuilder builder, boolean checkColumn) {
        builder.column("d.description");
        builder.join(DESCRIPTION_JOIN.formatted(classId, checkColumn ? "AND d.objsubid = 0" : ""));
    }

    protected void addMsPriviligesPart(QueryBuilder builder) {
        var subSelect = formatMsPriviliges(MS_PRIVILEGES_JOIN_SUBSELECT.copy());
        builder
                .column("aa.acl")
                .join("CROSS APPLY", subSelect, "aa (acl)");
    }

    protected QueryBuilder formatMsPriviliges(QueryBuilder privileges) {
        return privileges
                .column("col.name AS c")
                .where("major_id = res.object_id")
                .where("perm.class = 1");
    }

    protected void addMsOwnerPart(QueryBuilder builder) {
        addMsOwnerPart("res.principal_id", builder);
    }

    protected void addMsOwnerPart(String field, QueryBuilder builder) {
        builder.column("p.name AS owner");
        builder.join("JOIN sys.database_principals p WITH (NOLOCK) ON p.principal_id=" + field);
    }

    protected abstract void processResult(ResultSet result) throws SQLException, XmlReaderException;

    protected abstract void fillQueryBuilder(QueryBuilder builder);

    protected void setParams(PreparedStatement statement) throws SQLException {
        // subclasses will override if needed
    }

    /**
     * Override for postgres.
     *
     * @return object class's catalog name
     */
    protected String getClassId() {
        return null;
    }
}
