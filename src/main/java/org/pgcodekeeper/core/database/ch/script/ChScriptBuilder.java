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
package org.pgcodekeeper.core.database.ch.script;

import java.util.List;
import java.util.Set;
import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.database.base.script.AbstractScriptBuilder;
import org.pgcodekeeper.core.model.difftree.TreeElement;
import org.pgcodekeeper.core.model.graph.ActionContainer;
import org.pgcodekeeper.core.model.graph.ActionsToScriptConverter;
import org.pgcodekeeper.core.script.SQLScript;
import org.pgcodekeeper.core.settings.DiffSettings;

public class ChScriptBuilder extends AbstractScriptBuilder {

    public ChScriptBuilder(DiffSettings diffSettings) {
        super(diffSettings);
    }

    @Override
    protected String getScript(Set<ActionContainer> actions, Set<IStatement> toRefresh,
                               List<TreeElement> selected,
                               IDatabase oldDb, IDatabase newDb) {
        SQLScript script = new SQLScript(getSettings(), oldDb.getSeparator());
        ActionsToScriptConverter.fillScript(script, actions, toRefresh, oldDb, newDb, selected);
        return script.getFullScript();
    }
}
