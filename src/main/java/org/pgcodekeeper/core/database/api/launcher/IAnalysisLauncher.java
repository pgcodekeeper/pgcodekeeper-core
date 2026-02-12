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
package org.pgcodekeeper.core.database.api.launcher;

import org.pgcodekeeper.core.database.api.schema.GenericColumn;
import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.database.api.schema.ObjectLocation;
import org.pgcodekeeper.core.database.api.schema.meta.IMetaContainer;
import java.util.List;
import java.util.Set;

/**
 * This class and all child classes contains statement, its contexts and
 * implementation of logic for launch the analysis of statement's contexts.
 */
public interface IAnalysisLauncher {

    IStatement getStmt();

    void updateStmt(IDatabase database);

    Set<GenericColumn> launchAnalyze(List<Object> errors, IMetaContainer meta);

    List<ObjectLocation> getReferences();

    String getSchemaName();
}
