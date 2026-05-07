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
import org.pgcodekeeper.core.database.base.project.AbstractWorkDirs;
import org.pgcodekeeper.core.database.base.project.DirRule;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Directory structure definitions for ClickHouse project loader.
 */
public class ChWorkDirs extends AbstractWorkDirs {

    public static final String DATABASE = "DATABASE";

    /**
     * Creates ChWorkDirs with default directory structure only.
     */
    public ChWorkDirs() {
        super(null);
    }

    /**
     * Creates ChWorkDirs and applies overrides from the given alt-dirs properties file.
     *
     * @param altDirsFile path to the alt-dirs properties file (any filename),
     *                    or {@code null} for defaults only
     */
    public ChWorkDirs(Path altDirsFile) {
        super(altDirsFile);
    }

    @Override
    protected Map<String, DirRule> getDefaultDirNames() {
        Map<String, DirRule> result = new LinkedHashMap<>();
        result.put("SCHEMA", new DirRule(DATABASE, DbObjType.SCHEMA, false));
        result.put("TABLE", new DirRule("TABLE", DbObjType.TABLE, true));
        result.put("VIEW", new DirRule("VIEW", DbObjType.VIEW, true));
        result.put("DICTIONARY", new DirRule("DICTIONARY", DbObjType.DICTIONARY, true));
        result.put("FUNCTION", new DirRule("FUNCTION", DbObjType.FUNCTION, false));
        result.put("USER", new DirRule("USER", DbObjType.USER, false));
        result.put("POLICY", new DirRule("POLICY", DbObjType.POLICY, false));
        result.put("ROLE", new DirRule("ROLE", DbObjType.ROLE, false));
        return result;
    }

    @Override
    protected boolean isSplitBySchemaByDefault() {
        return true;
    }
}
