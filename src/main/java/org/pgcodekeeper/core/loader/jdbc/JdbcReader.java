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

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.PgDiffUtils;
import org.pgcodekeeper.core.Utils;
import org.pgcodekeeper.core.loader.QueryBuilder;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.base.QNameParser;
import org.pgcodekeeper.core.database.base.schema.AbstractSchema;
import org.pgcodekeeper.core.database.api.schema.GenericColumn;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.BiConsumer;

/**
 * Abstract base class for JDBC readers that process database objects within schemas.
 * Extends AbstractStatementReader to provide schema-aware processing and dependency management.
 */
public abstract class JdbcReader extends AbstractStatementReader {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcReader.class);

    private static final String EXTENSIONS_SCHEMAS = "extensions_schemas";

    private static final QueryBuilder EXTENSION_SCHEMA_CTE_SUBSELECT = new QueryBuilder()
            .column("1")
            .from("pg_catalog.pg_depend dp")
            .where("dp.objid = n.oid")
            .where("dp.deptype = 'e'")
            .where("dp.classid = 'pg_catalog.pg_namespace'::pg_catalog.regclass");

    private static final QueryBuilder EXTENSION_SCHEMA_CTE = new QueryBuilder()
            .column("n.oid")
            .from("pg_catalog.pg_namespace n")
            .where("EXISTS", EXTENSION_SCHEMA_CTE_SUBSELECT);

    protected JdbcReader(JdbcLoaderBase loader) {
        super(loader);
    }

    @Override
    protected void processResult(ResultSet result) throws SQLException, XmlReaderException {
        String schemaColumn = getSchemaColumn();
        var schemaId = result.getObject(schemaColumn.substring(schemaColumn.indexOf('.') + 1));
        AbstractSchema schema = loader.getSchema(schemaId);
        if (schema == null) {
            var msg = "No schema found for id %s".formatted(schemaId);
            LOG.warn(msg);
            return;
        }

        try {
            processResult(result, schema);
        } catch (ConcurrentModificationException ex) {
            if (!loader.getSettings().isIgnoreConcurrentModification()) {
                throw ex;
            }
            LOG.error(ex.getLocalizedMessage(), ex);
        }
    }

    @Override
    protected QueryBuilder makeQuery() {
        String schemas = loader.getSchemas();
        if (schemas.isBlank()) {
            return null;
        }

        QueryBuilder builder = super.makeQuery();
        builder.column(getSchemaColumn());

        builder.where(getSchemaColumn() + " IN (" + schemas + ')');

        return builder;
    }

    /**
     * Checks validity of database objects that may be concurrently modified.
     * Functions that accept OID/object name and return metadata are considered unsafe
     * as they can return null if the object was deleted outside the transaction block.
     *
     * @param object the object to check for validity
     * @param type   the database object type
     * @param name   the object name
     * @throws ConcurrentModificationException if the object is null (was deleted)
     */
    public static void checkObjectValidity(Object object, DbObjType type, String name) {
        if (object == null) {
            throw new ConcurrentModificationException(
                    "Statement concurrent modification: " + type + ' ' + name);
        }
    }

    /**
     * Checks type validity for concurrent modifications.
     * Validates that the type is not null or unknown (???) which can occur
     * when functions process metadata of concurrently modified objects.
     *
     * @param type the type string to validate
     * @throws ConcurrentModificationException if the type is invalid
     */
    public static void checkTypeValidity(String type) {
        checkObjectValidity(type, DbObjType.TYPE, "");
        if ("???".equals(type) || "???[]".equals(type)) {
            throw new ConcurrentModificationException("Concurrent type modification");
        }
    }

    public static <T> T[] getColArray(ResultSet rs, String columnName) throws SQLException {
        return getColArray(rs, columnName, false);
    }

    /**
     * Retrieves an array column from the result set.
     * Returns the array values if present, or handles null values based on the allowed null flag.
     *
     * @param <T> the array element type
     * @param rs the result set containing the data
     * @param columnName the name of the column containing the array
     * @param isAllowedNull if true, returns null when column value is null;
     *                      if false, throws IllegalArgumentException when column value is null
     * @return the array values from the specified column, or null if the column value is null and nulls are allowed
     * @throws SQLException if array retrieval from the result set fails
     * @throws IllegalArgumentException if the column value is null and nulls are not allowed
     */
    public static <T> T[] getColArray(ResultSet rs, String columnName, boolean isAllowedNull) throws SQLException {
        Array arr = rs.getArray(columnName);
        if (arr != null) {
            @SuppressWarnings("unchecked")
            T[] ret = (T[]) arr.getArray();
            return ret;
        }

        if (isAllowedNull) {
            return null;
        }
        String callerClassName = Thread.currentThread().getStackTrace()[2].getFileName();
        throw new IllegalArgumentException(Messages.JdbcReader_column_null_value_error_message.formatted(columnName, callerClassName));
    }

    /**
     * @deprecated {@link #setFunctionWithDep(BiConsumer, AbstractStatement, String, String)}
     */
    @Deprecated
    protected <T extends AbstractStatement> void setFunctionWithDep(
            BiConsumer<T, String> setter, T statement, String function) {
        setFunctionWithDep(setter, statement, function, null);
    }

    /**
     * Sets a function reference on a statement and adds appropriate dependencies.
     * Parses the function name to extract schema information and adds schema and function dependencies.
     *
     * @param <T>       the statement type
     * @param setter    the setter to apply the function value
     * @param statement the statement to add dependencies to
     * @param function  the function name (possibly schema-qualified)
     * @param signature the function signature, or null if not applicable
     */
    public static <T extends AbstractStatement> void setFunctionWithDep(
            BiConsumer<T, String> setter, T statement, String function, String signature) {
        if (function.indexOf('.') != -1) {
            QNameParser<ParserRuleContext> parser = QNameParser.parsePg(function);
            String schemaName = parser.getSchemaName();
            if (schemaName != null && !Utils.isPgSystemSchema(schemaName)) {
                statement.addDependency(new GenericColumn(schemaName, DbObjType.SCHEMA));
                String name = parser.getFirstName();
                if (signature != null) {
                    name = PgDiffUtils.getQuotedName(name) + signature;
                }
                statement.addDependency(new GenericColumn(schemaName, name, DbObjType.FUNCTION));
            }
        }
        setter.accept(statement, function);
    }

    protected void addDep(AbstractStatement statement, String schemaName, String name, DbObjType type) {
        if (schemaName != null && !Utils.isPgSystemSchema(schemaName)) {
            statement.addDependency(new GenericColumn(schemaName, DbObjType.SCHEMA));
            statement.addDependency(new GenericColumn(schemaName, name, type));
        }
    }

    protected abstract void processResult(ResultSet result, AbstractSchema schema)
            throws SQLException, XmlReaderException;

    protected abstract String getSchemaColumn();

    protected void addExtensionSchemasCte(QueryBuilder builder) {
        builder.with(EXTENSIONS_SCHEMAS, EXTENSION_SCHEMA_CTE);
        builder.where(getSchemaColumn() + " NOT IN (SELECT oid FROM extensions_schemas)");
    }

    @Override
    protected void addMsOwnerPart(String field, QueryBuilder builder) {
        builder.column("p.name AS owner");
        // left join
        builder.join("LEFT JOIN sys.database_principals p WITH (NOLOCK) ON p.principal_id=" + field);
    }

    protected String getTextWithCheckNewLines(String text) {
        return Utils.checkNewLines(text, loader.getSettings().isKeepNewlines());
    }
}
