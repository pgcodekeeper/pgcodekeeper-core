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

/**
 * Public view of a directory placement rule for database objects. Exposes only
 * the information required by project consumers (directory name and whether
 * objects of this rule are nested under a schema container). Matching details
 * and override mutation remain implementation concerns of the base layer.
 */
public interface IDirRule {

    /**
     * Returns the directory name where matching objects are stored.
     */
    String getDirName();

    /**
     * Returns {@code true} if objects of this rule are nested under a schema
     * container, {@code false} if they are placed at the top level.
     */
    boolean isSubElement();
}
