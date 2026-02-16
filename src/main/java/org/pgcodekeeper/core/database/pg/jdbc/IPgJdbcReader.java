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

import java.util.function.BiConsumer;

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.jdbc.QueryBuilder;
import org.pgcodekeeper.core.database.base.parser.QNameParser;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.database.pg.parser.PgParserUtils;
import org.pgcodekeeper.core.database.pg.utils.PgDiffUtils;
import org.pgcodekeeper.core.exception.ConcurrentModificationException;

public interface IPgJdbcReader {

    String EXTENSION_JOIN =
            "LEFT JOIN %s.dbots_event_data time ON time.objid = res.oid AND time.classid = %s::pg_catalog.regclass";

    String PG_CATALOG = "pg_catalog.";

    QueryBuilder EXTENSION_DEPS_CTE_BUILDER = new QueryBuilder()
            .column("objid")
            .from("pg_catalog.pg_depend");

    String DESCRIPTION_JOIN =
            "LEFT JOIN pg_catalog.pg_description d ON d.objoid = res.oid AND d.classoid = %s::pg_catalog.regclass %s";

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
    default <T extends AbstractStatement> void setFunctionWithDep(
            BiConsumer<T, String> setter, T statement, String function, String signature) {
        if (function.indexOf('.') != -1) {
            QNameParser<ParserRuleContext> parser = PgParserUtils.parseQName(function);
            String schemaName = parser.getSchemaName();
            if (schemaName != null && !PgDiffUtils.isSystemSchema(schemaName)) {
                statement.addDependency(new ObjectReference(schemaName, DbObjType.SCHEMA));
                String name = parser.getFirstName();
                if (signature != null) {
                    name = PgDiffUtils.getQuotedName(name) + signature;
                }
                statement.addDependency(new ObjectReference(schemaName, name, DbObjType.FUNCTION));
            }
        }
        setter.accept(statement, function);
    }

    /**
     * Checks type validity for concurrent modifications.
     * Validates that the type is not null or unknown (???) which can occur
     * when functions process metadata of concurrently modified objects.
     *
     * @param type the type string to validate
     * @throws ConcurrentModificationException if the type is invalid
     */
    static void checkTypeValidity(String type) {
        checkObjectValidity(type, DbObjType.TYPE, "");
        if ("???".equals(type) || "???[]".equals(type)) {
            throw new ConcurrentModificationException("Concurrent type modification");
        }
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
    static void checkObjectValidity(Object object, DbObjType type, String name) {
        if (object == null) {
            throw new ConcurrentModificationException(
                    "Statement concurrent modification: " + type + ' ' + name);
        }
    }

    default void appendExtension(QueryBuilder builder, String extensionSchema) {
        if (extensionSchema != null) {
            builder.column("time.ses_user");
            builder.join(EXTENSION_JOIN.formatted(PgDiffUtils.getQuotedName(extensionSchema), getFormattedClassId()));
        }
    }

    default QueryBuilder getExtensionCte() {
        return EXTENSION_DEPS_CTE_BUILDER.copy().where("deptype = 'e'");
    }

    default void addDescriptionPart(QueryBuilder builder) {
        addDescriptionPart(builder, false);
    }

    default void addDescriptionPart(QueryBuilder builder, boolean checkColumn) {
        builder.column("d.description");
        builder.join(DESCRIPTION_JOIN.formatted(getFormattedClassId(), checkColumn ? "AND d.objsubid = 0" : ""));
    }


    default void addExtensionDepsCte(QueryBuilder builder) {
        QueryBuilder subSelect = getExtensionCte()
                .where("classid = %s::pg_catalog.regclass".formatted(getFormattedClassId()));
        builder.with("extension_deps", subSelect);
        builder.where("res.oid NOT IN (SELECT objid FROM extension_deps)");
    }

    /**
     * Override for postgres.
     *
     * @return object class's catalog name
     */
    String getClassId();

    String getFormattedClassId();
}
