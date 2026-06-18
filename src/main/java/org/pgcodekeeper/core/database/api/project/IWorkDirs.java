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
package org.pgcodekeeper.core.database.api.project;

import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.api.schema.ISubElement;
import org.pgcodekeeper.core.database.api.schema.IStatement;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Project directory layout. Describes which directory name is associated with
 * each database object type and resolves per-object file locations.
 */
public interface IWorkDirs {

    String SCHEMA_KEY = "SCHEMA";

    /**
     * Returns {@code true} if objects are organized into subdirectories
     * per schema/database container, {@code false} if the schema name
     * is encoded in the filename instead.
     */
    boolean isSplitBySchema();

    /**
     * Returns the mapping of configurable type names to their placement rules.
     * Defines which object types participate in the project layout and may be
     * overridden by external configuration.
     * <p>
     * Keys are type names (e.g. {@code TABLE}, {@code VIEW}, {@code TRIGGER_FUNC}).
     * Values are {@link IDirRule} instances carrying the current directory name
     * (default or overridden) together with matching metadata.
     *
     * @return map of type names to their placement rules
     */
    Map<String, ? extends IDirRule> getDirMapping();

    /**
     * Computes the relative path (inside a project directory) where the given
     * statement should be stored. Walks up {@link ISubElement} chains so that
     * sub-elements share the file of their parent.
     *
     * @param st statement to locate
     * @return project-relative path to the statement's SQL file
     */
    Path getRelativeFilePath(IStatement st);

    /**
     * Returns the current directory name of the generic rule for the given type.
     * Specific-subset rules (e.g. {@code MAT_VIEW}, {@code TRIGGER_FUNC}) are skipped
     * so the result reflects where the "regular" objects of the type live.
     *
     * @param type object type
     * @return directory name, or {@code null} if no generic rule is registered
     */
    String getDirNameForType(DbObjType type);

    /**
     * Persists the current directory layout overrides to the given project.
     * Implementations write only entries that differ from the defaults, so an
     * unchanged layout may produce an empty file.
     *
     * @param projectPath target project directory; must exist
     * @throws IOException if the configuration file cannot be written
     */
    void saveAltDirs(Path projectPath) throws IOException;

    /**
     * Returns the distinct top-level directory names of this layout, i.e. the
     * directories created directly under the project root.
     *
     * @return distinct top-level directory names of this layout
     */
    default List<String> getTopLevelDirNames() {
        boolean split = isSplitBySchema();
        return getDirMapping().values().stream()
                .filter(rule -> !split || !rule.isSubElement())
                .map(rule -> rule.getDirName().split("/")[0])
                .filter(name -> !name.isBlank())
                .distinct()
                .toList();
    }
}
