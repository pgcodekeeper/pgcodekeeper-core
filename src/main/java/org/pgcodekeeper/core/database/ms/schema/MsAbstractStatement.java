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

import org.pgcodekeeper.core.database.api.formatter.IFormatConfiguration;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.database.ms.MsDiffUtils;
import org.pgcodekeeper.core.database.ms.formatter.MsFormatter;
import org.pgcodekeeper.core.script.SQLScript;
import org.pgcodekeeper.core.utils.Utils;

import java.util.Locale;
import java.util.function.UnaryOperator;

public abstract class MsAbstractStatement extends AbstractStatement {

    private static final String RENAME_OBJECT_COMMAND = "EXEC sp_rename %s, %s";
    private static final String GO = "\nGO";

    protected MsAbstractStatement(String name) {
        super(name);
    }

    @Override
    public String formatSql(String sql, int offset, int length, IFormatConfiguration formatConfiguration) {
        return new MsFormatter(sql, offset, length, formatConfiguration).formatText();
    }

    @Override
    public UnaryOperator<String> getQuoter() {
        return MsDiffUtils::quoteName;
    }

    @Override
    public void appendOwnerSQL(SQLScript script) {
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
    public boolean isOwned() {
        return getStatementType().in(DbObjType.TABLE, DbObjType.VIEW, DbObjType.SCHEMA, DbObjType.FUNCTION,
                DbObjType.PROCEDURE, DbObjType.SEQUENCE, DbObjType.TYPE, DbObjType.ASSEMBLY, DbObjType.STATISTICS);
    }

    @Override
    protected void alterOwnerSQL(SQLScript script) {
        if (owner == null) {
            StringBuilder sb = new StringBuilder();
            sb.append("ALTER AUTHORIZATION ON ");
            DbObjType type = getStatementType();
            if (DbObjType.TYPE == type || DbObjType.SCHEMA == type
                    || DbObjType.ASSEMBLY == type) {
                sb.append(type).append("::");
            }

            sb.append(getQualifiedName()).append(" TO ");

            if (DbObjType.SCHEMA == type || DbObjType.ASSEMBLY == type) {
                sb.append("[dbo]");
            } else {
                sb.append("SCHEMA OWNER");
            }

            script.addStatement(sb);
        } else {
            super.alterOwnerSQL(script);
        }
    }

    @Override
    protected String getNameInCorrectCase(String name) {
        return name.toLowerCase(Locale.ROOT);
    }

    @Override
    public String getRenameCommand(String newName) {
        return RENAME_OBJECT_COMMAND.formatted(Utils.quoteString(getQualifiedName()), Utils.quoteString(newName));
    }

    @Override
    public String getSeparator() {
        return GO;
    }
}
