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
package org.pgcodekeeper.core.database.ms.schema;

import java.util.function.UnaryOperator;

import org.pgcodekeeper.core.database.api.formatter.IFormatConfiguration;
import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.ms.MsDiffUtils;
import org.pgcodekeeper.core.database.ms.formatter.MsFormatter;
import org.pgcodekeeper.core.script.SQLScript;
import org.pgcodekeeper.core.utils.Utils;

/**
 * Interface for MS SQL statement
 */
public interface IMsStatement extends IStatement {

    String RENAME_OBJECT_COMMAND = "EXEC sp_rename %s, %s";
    String GO = "\nGO";

    @Override
    default String formatSql(String sql, int offset, int length, IFormatConfiguration formatConfiguration) {
        return new MsFormatter(sql, offset, length, formatConfiguration).formatText();
    }

    @Override
    default DatabaseType getDbType() {
        return DatabaseType.MS;
    }

    @Override
    default UnaryOperator<String> getQuoter() {
        return MsDiffUtils::quoteName;
    }

    @Override
    default void appendOwnerSQL(SQLScript script) {
        var owner = getOwner();
        if (owner == null || !isOwned()) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER AUTHORIZATION ON ");

        DbObjType type = getStatementType();
        if (type.in(DbObjType.TYPE, DbObjType.SCHEMA, DbObjType.ASSEMBLY)) {
            sb.append(type).append("::");
        }

        sb.append(getQualifiedName()).append(" TO ").append(getQuotedName(owner));

        script.addStatement(sb);
    }

    @Override
    default boolean isOwned() {
        return getStatementType().in(DbObjType.TABLE, DbObjType.VIEW, DbObjType.SCHEMA, DbObjType.FUNCTION,
                DbObjType.PROCEDURE, DbObjType.SEQUENCE, DbObjType.TYPE, DbObjType.ASSEMBLY, DbObjType.STATISTICS);
    }

    @Override
    default void appendDefaultPrivileges(IStatement statement, SQLScript script) {
        // no imp
    }

    @Override
    default String getRenameCommand(String newName) {
        return RENAME_OBJECT_COMMAND.formatted(Utils.quoteString(getQualifiedName()), Utils.quoteString(newName));
    }
}
