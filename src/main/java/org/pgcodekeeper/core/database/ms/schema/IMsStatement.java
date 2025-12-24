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
package org.pgcodekeeper.core.database.ms.schema;

import org.pgcodekeeper.core.database.ms.MsDiffUtils;
import org.pgcodekeeper.core.database.api.formatter.IFormatConfiguration;
import org.pgcodekeeper.core.database.api.schema.DatabaseType;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.database.ms.formatter.MsFormatter;
import org.pgcodekeeper.core.script.SQLScript;

import java.util.function.UnaryOperator;

/**
 * Interface for MS SQL statement
 */
public interface IMsStatement extends IStatement {

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
    default void appendDefaultPrivileges(IStatement statement, SQLScript script) {
        // no imp
    }
}
