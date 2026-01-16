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
package org.pgcodekeeper.core.database.pg.schema;

import java.util.function.UnaryOperator;

import org.pgcodekeeper.core.database.api.formatter.IFormatConfiguration;
import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.pg.PgDiffUtils;
import org.pgcodekeeper.core.database.pg.formatter.PgFormatter;
import org.pgcodekeeper.core.script.SQLScript;

/**
 * Interface for PostgreSQL statement
 */
public interface IPgStatement extends IStatement {

    String RENAME_OBJECT_COMMAND = "ALTER %s %s RENAME TO %s";

    @Override
    default String formatSql(String sql, int offset, int length, IFormatConfiguration formatConfiguration) {
        return new PgFormatter(sql, offset, length, formatConfiguration).formatText();
    }

    @Override
    default DatabaseType getDbType() {
        return DatabaseType.PG;
    }

    @Override
    default UnaryOperator<String> getQuoter() {
        return PgDiffUtils::getQuotedName;
    }

    @Override
    default void appendDefaultPrivileges(IStatement statement, SQLScript script) {
        // reset all default privileges
        // this generates noisier bit more correct scripts
        // we may have revoked implicit owner GRANT in the previous step, it needs to be restored
        // any privileges in non-default state will be set to their final state in the next step
        // this solution also requires the least amount of handling code: no edge cases
        PgPrivilege.appendDefaultPostgresPrivileges(statement, script);
    }

    @Override
    default String getRenameCommand(String newName) {
        return RENAME_OBJECT_COMMAND.formatted(getStatementType(), getQualifiedName(), getQuotedName(newName));
    }

    /**
     * Wraps SQL in DO block with error handling.
     *
     * @param sbResult        the StringBuilder to append to
     * @param sbSQL           the SQL to wrap
     * @param expectedErrCode the expected error code to handle
     */
    default void appendSqlWrappedInDo(StringBuilder sbResult, StringBuilder sbSQL, String expectedErrCode) {
        String body = sbSQL.toString().replace("\n", "\n\t");

        sbResult
                .append("DO $$")
                .append("\nBEGIN")
                .append("\n\t").append(body)
                .append("\nEXCEPTION WHEN OTHERS THEN")
                .append("\n\tIF (SQLSTATE = ").append(expectedErrCode).append(") THEN")
                .append("\n\t\tRAISE NOTICE '%, skip', SQLERRM;")
                .append("\n\tELSE")
                .append("\n\t\tRAISE;")
                .append("\n\tEND IF;")
                .append("\nEND; $$")
                .append("\nLANGUAGE 'plpgsql'");
    }
}
