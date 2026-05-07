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
import org.pgcodekeeper.core.database.base.project.AbstractWorkDirs;
import org.pgcodekeeper.core.database.base.project.DirRule;
import org.pgcodekeeper.core.database.pg.schema.PgAbstractForeignTable;
import org.pgcodekeeper.core.database.pg.schema.PgFunction;
import org.pgcodekeeper.core.database.pg.schema.PgMaterializedView;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Directory structure definitions for PostgreSQL project loader.
 */
public class PgWorkDirs extends AbstractWorkDirs {

    /**
     * Creates PgWorkDirs with default directory structure only.
     */
    public PgWorkDirs() {
        super(null);
    }

    /**
     * Creates PgWorkDirs and applies overrides from the given alt-dirs properties file.
     * Supports standard {@link DbObjType} keys and the following custom keys:
     * <ul>
     *   <li>{@code MAT_VIEW} — materialized views (subset of {@code VIEW})</li>
     *   <li>{@code FOREIGN_TABLE} — foreign tables (subset of {@code TABLE})</li>
     *   <li>{@code TRIGGER_FUNC} — trigger functions (subset of {@code FUNCTION})</li>
     * </ul>
     *
     * @param altDirsFile path to the alt-dirs properties file (any filename),
     *                    or {@code null} for defaults only
     */
    public PgWorkDirs(Path altDirsFile) {
        super(altDirsFile);
    }

    @Override
    protected Map<String, DirRule> getDefaultDirNames() {
        Map<String, DirRule> result = new LinkedHashMap<>();
        result.put("SCHEMA", new DirRule("SCHEMA", DbObjType.SCHEMA, false));
        result.put("COLLATION", new DirRule("COLLATION", DbObjType.COLLATION, true));
        result.put("TYPE", new DirRule("TYPE", DbObjType.TYPE, true));
        result.put("DOMAIN", new DirRule("DOMAIN", DbObjType.DOMAIN, true));
        result.put("SEQUENCE", new DirRule("SEQUENCE", DbObjType.SEQUENCE, true));
        result.put("TRIGGER_FUNC", new DirRule("FUNCTION", DbObjType.FUNCTION, true, true, st -> st instanceof PgFunction f && f.isTriggerFunction()));
        result.put("FUNCTION", new DirRule("FUNCTION", DbObjType.FUNCTION, true));
        result.put("PROCEDURE", new DirRule("PROCEDURE", DbObjType.PROCEDURE, true));
        result.put("AGGREGATE", new DirRule("AGGREGATE", DbObjType.AGGREGATE, true));
        result.put("OPERATOR", new DirRule("OPERATOR", DbObjType.OPERATOR, true));
        result.put("FOREIGN_TABLE", new DirRule("TABLE", DbObjType.TABLE, true, true, PgAbstractForeignTable.class::isInstance));
        result.put("TABLE", new DirRule("TABLE", DbObjType.TABLE, true));
        result.put("MAT_VIEW", new DirRule("VIEW", DbObjType.VIEW, true, true, PgMaterializedView.class::isInstance));
        result.put("VIEW", new DirRule("VIEW", DbObjType.VIEW, true));
        result.put("STATISTICS", new DirRule("STATISTICS", DbObjType.STATISTICS, true));
        result.put("FTS_PARSER", new DirRule("FTS_PARSER", DbObjType.FTS_PARSER, true));
        result.put("FTS_TEMPLATE", new DirRule("FTS_TEMPLATE", DbObjType.FTS_TEMPLATE, true));
        result.put("FTS_DICTIONARY", new DirRule("FTS_DICTIONARY", DbObjType.FTS_DICTIONARY, true));
        result.put("FTS_CONFIGURATION", new DirRule("FTS_CONFIGURATION", DbObjType.FTS_CONFIGURATION, true));
        result.put("EXTENSION", new DirRule("EXTENSION", DbObjType.EXTENSION, false));
        result.put("EVENT_TRIGGER", new DirRule("EVENT_TRIGGER", DbObjType.EVENT_TRIGGER, false));
        result.put("USER_MAPPING", new DirRule("USER_MAPPING", DbObjType.USER_MAPPING, false));
        result.put("CAST", new DirRule("CAST", DbObjType.CAST, false));
        result.put("SERVER", new DirRule("SERVER", DbObjType.SERVER, false));
        result.put("FOREIGN_DATA_WRAPPER", new DirRule("FDW", DbObjType.FOREIGN_DATA_WRAPPER, false));
        return result;
    }

    @Override
    protected boolean isSplitBySchemaByDefault() {
        return true;
    }
}
