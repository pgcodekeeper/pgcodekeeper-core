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
package org.pgcodekeeper.core.database.ch.project;

import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.utils.FileUtils;

import java.nio.file.Path;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Directory structure definitions for ClickHouse project loader.
 */
public final class ChWorkDirs {

    public static final String DATABASE = "DATABASE";

    private static final List<String> DIRECTORY_NAMES = List.of(
            DATABASE, "FUNCTION", "USER", "POLICY", "ROLE");

    private static final EnumSet<DbObjType> DIR_LOAD_ORDER = EnumSet.of(DbObjType.TABLE, DbObjType.VIEW,
            DbObjType.DICTIONARY);

    public static List<String> getDirectoryNames() {
        return DIRECTORY_NAMES;
    }

    public static Set<DbObjType> getDirLoadOrder() {
        return DIR_LOAD_ORDER;
    }

    /**
     * Gets the relative filesystem path for a ClickHouse database statement.
     *
     * @param st      the database statement
     * @param baseDir the base directory path
     * @return relative path where the statement should be stored
     * @throws IllegalStateException if the object type is not supported
     */
    public static Path getRelativeFolderPath(IStatement st, Path baseDir) {
        DbObjType type = st.getStatementType();
        return switch (type) {
            case USER, ROLE, FUNCTION, POLICY -> baseDir.resolve(getDirectoryNameForType(type));
            case SCHEMA -> {
                String databaseName = FileUtils.getValidFilename(st.getBareName());
                yield baseDir.resolve(DATABASE).resolve(databaseName);
            }
            case TABLE, DICTIONARY, VIEW -> {
                var parentSt = st.getParent();
                String databaseName = FileUtils.getValidFilename(parentSt.getBareName());
                yield baseDir.resolve(DATABASE).resolve(databaseName).resolve(getDirectoryNameForType(type));
            }
            default -> throw new IllegalStateException(Messages.DbObjType_unsupported_type + type);
        };
    }

    /**
     * Gets the directory name for a ClickHouse database object type.
     *
     * @param type the database object type
     * @return directory name for the type
     * @throws IllegalStateException if the object type is not supported
     */
    public static String getDirectoryNameForType(DbObjType type) {
        return switch (type) {
            case SCHEMA -> DATABASE;
            case FUNCTION, POLICY, USER, ROLE, TABLE, DICTIONARY, VIEW -> type.name();
            case CONSTRAINT, INDEX, COLUMN -> null;
            default -> throw new IllegalStateException(Messages.DbObjType_unsupported_type + type);
        };
    }

    private ChWorkDirs() {
    }
}
