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
 **
 * Copyright 2006 StartNet s.r.o.
 *
 * Distributed under MIT license
 *******************************************************************************/
package org.pgcodekeeper.core.schema;

import org.pgcodekeeper.core.model.difftree.DbObjType;

/**
 * Interface for database objects that can contain other statements as children.
 * Provides functionality for adding and retrieving child statements.
 */
public interface IStatementContainer extends IStatement {

    /**
     * Adds a child statement to this container.
     *
     * @param stmt the child statement to add
     */
    void addChild(IStatement stmt);

    /**
     * Gets a child statement by name and type.
     *
     * @param name the name of the child to find
     * @param type the type of the child to find
     * @return the child statement, or null if not found
     */
    PgStatement getChild(String name, DbObjType type);
}
