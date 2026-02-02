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
package org.pgcodekeeper.core.database.pg.project;

import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.utils.FileUtils;

import java.nio.file.Path;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Directory structure definitions for PostgreSQL project loader.
 */
public final class PgWorkDirs {

    public static final String SCHEMA = "SCHEMA";

    private static final List<String> DIRECTORY_NAMES = List.of(
            SCHEMA, "EXTENSION", "EVENT_TRIGGER", "USER_MAPPING", "CAST", "SERVER", "FDW");

    private static final EnumSet<DbObjType> DIR_LOAD_ORDER = EnumSet.of(
            DbObjType.COLLATION, DbObjType.TYPE, DbObjType.DOMAIN, DbObjType.SEQUENCE,
            DbObjType.FUNCTION, DbObjType.PROCEDURE, DbObjType.AGGREGATE, DbObjType.OPERATOR,
            DbObjType.TABLE, DbObjType.VIEW, DbObjType.STATISTICS, DbObjType.DICTIONARY,
            DbObjType.FTS_PARSER, DbObjType.FTS_TEMPLATE, DbObjType.FTS_DICTIONARY, DbObjType.FTS_CONFIGURATION);

    public static List<String> getDirectoryNames() {
        return DIRECTORY_NAMES;
    }

    public static Set<DbObjType> getDirLoadOrder() {
        return DIR_LOAD_ORDER;
    }

    /**
     * Gets the relative filesystem path for a PostgreSQL database statement.
     *
     * @param st      the database statement
     * @param baseDir the base directory path
     * @return relative path where the statement should be stored
     * @throws IllegalStateException if the object type is not supported
     */
    public static Path getRelativeFolderPath(IStatement st, Path baseDir) {
        DbObjType type = st.getStatementType();
        return switch (type) {
            case EXTENSION, SERVER, USER_MAPPING, CAST, EVENT_TRIGGER, FOREIGN_DATA_WRAPPER -> baseDir
                    .resolve(getDirectoryNameForType(type));
            case SCHEMA -> {
                String schemaName = FileUtils.getValidFilename(st.getBareName());
                yield baseDir.resolve(SCHEMA).resolve(schemaName);
            }
            case COLLATION, SEQUENCE, TYPE, DOMAIN, VIEW, TABLE, FUNCTION, PROCEDURE, AGGREGATE, OPERATOR,
                 FTS_TEMPLATE, FTS_PARSER, FTS_DICTIONARY, FTS_CONFIGURATION, STATISTICS, COLUMN -> {
                var parentSt = st.getParent();
                String schemaName = FileUtils.getValidFilename(parentSt.getBareName());
                if (type == DbObjType.COLUMN) {
                    type = DbObjType.TABLE;
                }
                yield baseDir.resolve(SCHEMA).resolve(schemaName).resolve(getDirectoryNameForType(type));
            }
            default -> throw new IllegalStateException(Messages.DbObjType_unsupported_type + type);
        };
    }

    /**
     * Gets the directory name for a PostgreSQL database object type.
     *
     * @param type the database object type
     * @return directory name for the type
     * @throws IllegalStateException if the object type is not supported
     */
    public static String getDirectoryNameForType(DbObjType type) {
        return switch (type) {
            case FOREIGN_DATA_WRAPPER -> "FDW";
            case CONSTRAINT, INDEX, RULE, TRIGGER, POLICY, COLUMN -> null;
            case EXTENSION, SERVER, USER_MAPPING, CAST, EVENT_TRIGGER, SCHEMA, COLLATION, SEQUENCE, TYPE, DOMAIN,
                 VIEW, TABLE, FUNCTION, PROCEDURE, AGGREGATE, OPERATOR, FTS_TEMPLATE, FTS_PARSER, FTS_DICTIONARY,
                 FTS_CONFIGURATION, STATISTICS -> type.name();
            default -> throw new IllegalStateException(Messages.DbObjType_unsupported_type + type);
        };
    }

    private PgWorkDirs() {
    }
}
