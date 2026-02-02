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
package org.pgcodekeeper.core.database.ms.project;

import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.localizations.Messages;

import java.nio.file.Path;
import java.util.List;

/**
 * Directory structure definitions for MS SQL Server project loader.
 */
public final class MsWorkDirs {

    public static final String SECURITY = "Security";
    public static final String SCHEMAS = "Schemas";
    public static final String USERS = "Users";
    public static final String ROLES = "Roles";

    private static final String ASSEMBLIES = "Assemblies";

    private static final List<String> DIRECTORY_NAMES = List.of(
            ASSEMBLIES, "Types", "Tables", "Views", "Sequences",
            "Functions", "Stored Procedures", SECURITY);

    public static List<String> getDirectoryNames() {
        return DIRECTORY_NAMES;
    }

    public static boolean isInSchema(String dirSub) {
        return !ASSEMBLIES.equals(dirSub) && !SECURITY.equals(dirSub);
    }

    /**
     * Gets the relative filesystem path for a MS SQL Server database statement.
     *
     * @param st      the database statement
     * @param baseDir the base directory path
     * @return relative path where the statement should be stored
     * @throws IllegalStateException if the object type is not supported
     */
    public static Path getRelativeFolderPath(IStatement st, Path baseDir) {
        DbObjType type = st.getStatementType();
        return switch (type) {
            case SCHEMA, ROLE, USER -> baseDir.resolve(SECURITY).resolve(getDirectoryNameForType(type));
            case ASSEMBLY, SEQUENCE, VIEW, TABLE, FUNCTION, PROCEDURE, TYPE -> baseDir
                    .resolve(getDirectoryNameForType(type));
            default -> throw new IllegalStateException(Messages.DbObjType_unsupported_type + type);
        };
    }

    /**
     * Gets the directory name for a MS SQL Server database object type.
     *
     * @param type the database object type
     * @return directory name for the type
     * @throws IllegalStateException if the object type is not supported
     */
    public static String getDirectoryNameForType(DbObjType type) {
        return switch (type) {
            case SCHEMA -> SCHEMAS;
            case ROLE -> ROLES;
            case USER -> USERS;
            case ASSEMBLY -> ASSEMBLIES;
            case SEQUENCE -> "Sequences";
            case VIEW -> "Views";
            case TABLE -> "Tables";
            case FUNCTION -> "Functions";
            case PROCEDURE -> "Stored Procedures";
            case TYPE -> "Types";
            case CONSTRAINT, INDEX, TRIGGER, COLUMN, STATISTICS -> null;
            default -> throw new IllegalStateException(Messages.DbObjType_unsupported_type + type);
        };
    }

    private MsWorkDirs() {
    }
}
