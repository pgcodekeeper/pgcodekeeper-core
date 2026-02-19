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
 **
 * Copyright 2006 StartNet s.r.o.
 *
 * Distributed under MIT license
 *******************************************************************************/
package org.pgcodekeeper.core.database.base.script;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;

import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.database.api.schema.ITable;
import org.pgcodekeeper.core.database.api.script.IScriptBuilder;
import org.pgcodekeeper.core.ignorelist.IgnoreList;
import org.pgcodekeeper.core.model.difftree.CompareTree;
import org.pgcodekeeper.core.model.difftree.DiffTree;
import org.pgcodekeeper.core.model.difftree.TreeElement;
import org.pgcodekeeper.core.model.difftree.TreeFlattener;
import org.pgcodekeeper.core.model.difftree.TreeElement.DiffSide;
import org.pgcodekeeper.core.model.graph.ActionContainer;
import org.pgcodekeeper.core.model.graph.DbObject;
import org.pgcodekeeper.core.model.graph.DepcyResolver;
import org.pgcodekeeper.core.settings.DiffSettings;
import org.pgcodekeeper.core.settings.ISettings;

public abstract class AbstractScriptBuilder implements IScriptBuilder {

    private static final String EMPTY_SCRIPT = ""; // $NON-NLS-1$

    private final DiffSettings diffSettings;

    /**
     * Creates a new AbstractScriptBuilder instance with the specified settings.
     *
     * @param diffSettings unified context object containing settings, ignore list,
     *                     and error accumulator
     */
    protected AbstractScriptBuilder(DiffSettings diffSettings) {
        this.diffSettings = diffSettings;
    }

    @Override
    public String createScript(TreeElement root, IDatabase oldDb, IDatabase newDb) throws IOException {
        List<TreeElement> selected = getSelectedElements(root, diffSettings.getIgnoreList());
        if (selected.isEmpty()) {
            return EMPTY_SCRIPT;
        }

        Set<IStatement> toRefresh = new LinkedHashSet<>();
        var actions = resolveDependencies(selected, oldDb, newDb, diffSettings.getAdditionalDependencies(),
                diffSettings.getAdditionalDependencies(), toRefresh);
        if (actions.isEmpty()) {
            return EMPTY_SCRIPT;
        }

        return getScript(actions, toRefresh, selected, oldDb, newDb);
    }

    private List<TreeElement> getSelectedElements(TreeElement root, IgnoreList ignoreList) {
        return new TreeFlattener()
                .onlySelected()
                .useIgnoreList(ignoreList)
                .onlyTypes(getSettings().getAllowedTypes())
                .flatten(root);
    }

    private Set<ActionContainer> resolveDependencies(List<TreeElement> selected, IDatabase oldDb, IDatabase newDb,
            List<Entry<IStatement, IStatement>> additionalDependenciesOldDb,
            List<Entry<IStatement, IStatement>> additionalDependenciesNewDb, Set<IStatement> toRefresh) {
        addColumnsAsElements(oldDb, newDb, selected);

        selected.sort(new CompareTree());

        List<DbObject> objects = new ArrayList<>();
        for (TreeElement st : selected) {
            IStatement oldStatement = null;
            IStatement newStatement = null;
            switch (st.getSide()) {
            case LEFT:
                oldStatement = st.getStatement(oldDb);
                break;
            case BOTH:
                oldStatement = st.getStatement(oldDb);
                newStatement = st.getStatement(newDb);
                break;
            case RIGHT:
                newStatement = st.getStatement(newDb);
                break;
            }
            objects.add(new DbObject(oldStatement, newStatement));
        }
        return DepcyResolver.resolve(oldDb, newDb, additionalDependenciesOldDb, additionalDependenciesNewDb, toRefresh,
                objects, getSettings());
    }

    @Deprecated
    private void addColumnsAsElements(IDatabase oldDb, IDatabase newDb, List<TreeElement> selected) {
        List<TreeElement> tempColumns = new ArrayList<>();
        for (TreeElement el : selected) {
            if (el.getType() == DbObjType.TABLE && el.getSide() == DiffSide.BOTH) {
                ITable oldTbl = (ITable) el.getStatement(oldDb);
                ITable newTbl = (ITable) el.getStatement(newDb);
                DiffTree.addColumns(oldTbl.getColumns(), newTbl.getColumns(), el, tempColumns);
            }
        }
        selected.addAll(tempColumns);
    }

    protected ISettings getSettings() {
        return diffSettings.getSettings();
    }

    protected abstract String getScript(Set<ActionContainer> actions, Set<IStatement> toRefresh,
                                        List<TreeElement> selected,
                                        IDatabase oldDb, IDatabase newDb) throws IOException;
}
