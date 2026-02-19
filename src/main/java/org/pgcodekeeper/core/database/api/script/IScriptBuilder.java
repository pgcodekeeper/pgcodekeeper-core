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
package org.pgcodekeeper.core.database.api.script;

import java.io.IOException;

import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.model.difftree.TreeElement;

/**
 * Interface for script builder
 */
public interface IScriptBuilder {

    /**
     * Gets selected elements from root, compares them between source and target
     * and generates a migration script.
     *
     * @param root  the root of the diff tree
     * @param oldDb the source database schema
     * @param newDb the target database schema
     * @return SQL migration script
     * @throws IOException if an I/O error occurs
     */
    public String createScript(TreeElement root, IDatabase oldDb, IDatabase newDb) throws IOException;
}
