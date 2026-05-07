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
import org.pgcodekeeper.core.database.base.project.AbstractWorkDirs;
import org.pgcodekeeper.core.database.base.project.DirRule;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Directory structure definitions for MS SQL Server project loader.
 */
public class MsWorkDirs extends AbstractWorkDirs {

    public static final String SECURITY = "Security";
    public static final String SCHEMAS = "Schemas";

    /**
     * Creates MsWorkDirs with default directory structure only.
     */
    public MsWorkDirs() {
        super(null);
    }

    /**
     * Creates MsWorkDirs and applies overrides from the given alt-dirs properties file.
     *
     * @param altDirsFile path to the alt-dirs properties file (any filename),
     *                    or {@code null} for defaults only
     */
    public MsWorkDirs(Path altDirsFile) {
        super(altDirsFile);
    }

    @Override
    protected Map<String, DirRule> getDefaultDirNames() {
        Map<String, DirRule> result = new LinkedHashMap<>();
        result.put("SCHEMA", new DirRule(SECURITY + '/' + SCHEMAS, DbObjType.SCHEMA, false));
        result.put("ROLE", new DirRule(SECURITY + "/Roles", DbObjType.ROLE, false));
        result.put("USER", new DirRule(SECURITY + "/Users", DbObjType.USER, false));
        result.put("ASSEMBLY", new DirRule("Assemblies", DbObjType.ASSEMBLY, false));
        result.put("TYPE", new DirRule("Types", DbObjType.TYPE, true));
        result.put("TABLE", new DirRule("Tables", DbObjType.TABLE, true));
        result.put("VIEW", new DirRule("Views", DbObjType.VIEW, true));
        result.put("SEQUENCE", new DirRule("Sequences", DbObjType.SEQUENCE, true));
        result.put("FUNCTION", new DirRule("Functions", DbObjType.FUNCTION, true));
        result.put("PROCEDURE", new DirRule("Stored Procedures", DbObjType.PROCEDURE, true));
        return result;
    }

    @Override
    protected boolean isSplitBySchemaByDefault() {
        return false;
    }
}
