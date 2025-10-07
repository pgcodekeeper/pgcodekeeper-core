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
package org.pgcodekeeper.core;

import org.pgcodekeeper.core.ignorelist.IgnoreList;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.model.difftree.*;
import org.pgcodekeeper.core.model.difftree.TreeElement.DiffSide;
import org.pgcodekeeper.core.model.graph.ActionContainer;
import org.pgcodekeeper.core.model.graph.ActionsToScriptConverter;
import org.pgcodekeeper.core.model.graph.DbObject;
import org.pgcodekeeper.core.model.graph.DepcyResolver;
import org.pgcodekeeper.core.schema.AbstractDatabase;
import org.pgcodekeeper.core.schema.AbstractTable;
import org.pgcodekeeper.core.schema.PgStatement;
import org.pgcodekeeper.core.script.SQLActionType;
import org.pgcodekeeper.core.script.SQLScript;
import org.pgcodekeeper.core.settings.ISettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Stream;

/**
 * Main class for generating SQL scripts that transform one database schema to another.
 * <p>
 * This class compares two database schemas (old and new) and generates the SQL
 * statements needed to migrate from the old schema to the new one.
 *
 * @author fordfrog
 */
public class PgDiff {

    protected static final Logger LOG = LoggerFactory.getLogger(PgDiff.class);

    private static final String EMPTY_SCRIPT = ""; // $NON-NLS-1$

    protected final ISettings settings;
    protected final List<Object> errors = new ArrayList<>();

    /**
     * Creates a new PgDiff instance with the specified settings.
     *
     * @param settings the configuration settings for the diff operation
     */
    public PgDiff(ISettings settings) {
        this.settings = settings;
    }

    public List<Object> getErrors() {
        return Collections.unmodifiableList(errors);
    }

    /**
     * Gets selected elements from root, compares them between source and target
     * and generates a migration script.
     *
     * @param root                        the root of the diff tree
     * @param oldDb                       the source database schema
     * @param newDb                       the target database schema
     * @param additionalDependenciesOldDb additional dependencies in old database
     * @param additionalDependenciesNewDb additional dependencies in new database
     * @param ignoreList                  list of objects to ignore during comparison
     * @return SQL migration script
     * @throws IOException if an I/O error occurs
     */
    public String diff(TreeElement root,
                       AbstractDatabase oldDb,
                       AbstractDatabase newDb,
                       List<Entry<PgStatement, PgStatement>> additionalDependenciesOldDb,
                       List<Entry<PgStatement, PgStatement>> additionalDependenciesNewDb,
                       IgnoreList ignoreList)
            throws IOException {
        List<TreeElement> selected = getSelectedElements(root, ignoreList);
        if (selected.isEmpty()) {
            return EMPTY_SCRIPT;
        }

        Set<PgStatement> toRefresh = new LinkedHashSet<>();
        var actions = resolveDependencies(selected, oldDb, newDb, additionalDependenciesOldDb,
                additionalDependenciesNewDb, toRefresh);
        if (actions.isEmpty()) {
            return EMPTY_SCRIPT;
        }

        return switch (settings.getDbType()) {
            case MS -> getMsScript(actions, toRefresh, selected, oldDb, newDb);
            case PG -> getPgScript(actions, toRefresh, selected, oldDb, newDb);
            case CH -> getChScript(actions, toRefresh, selected, oldDb, newDb);
        };
    }

    private String getPgScript(Set<ActionContainer> actions, Set<PgStatement> toRefresh, List<TreeElement> selected,
            AbstractDatabase oldDb, AbstractDatabase newDb)
            throws IOException {
        SQLScript script = new SQLScript(settings);
        for (String preFilePath : settings.getPreFilePath()) {
            addPrePostPath(script, preFilePath, SQLActionType.PRE);
        }

        if (settings.getTimeZone() != null) {
            script.addStatement("SET TIMEZONE TO " + PgDiffUtils.quoteString(settings.getTimeZone()), //$NON-NLS-1$
                    SQLActionType.BEGIN);
        }

        if (settings.isDisableCheckFunctionBodies()) {
            script.addStatement("SET check_function_bodies = false", SQLActionType.BEGIN); //$NON-NLS-1$
        }

        if (settings.isAddTransaction()) {
            script.addStatement("START TRANSACTION", SQLActionType.BEGIN); //$NON-NLS-1$
        }

        script.addStatement("SET search_path = pg_catalog", SQLActionType.BEGIN); //$NON-NLS-1$
        ActionsToScriptConverter.fillScript(script, actions, toRefresh, oldDb, newDb, selected);

        if (settings.isAddTransaction()) {
            script.addStatement("COMMIT TRANSACTION", SQLActionType.END); //$NON-NLS-1$
        }

        for (String postFilePath : settings.getPostFilePath()) {
            addPrePostPath(script, postFilePath, SQLActionType.POST);
        }
        return script.getFullScript();
    }

    private void addPrePostPath(SQLScript script, String scriptPath, SQLActionType actionType) throws IOException {
        Path path = Paths.get(scriptPath);
        addPrePostPath(script, path, actionType);
    }

    private void addPrePostPath(SQLScript script, Path path, SQLActionType actionType) throws IOException {
        if (Files.isRegularFile(path)) {
            addPrePostScript(script, path, actionType);
            return;
        }
        Stream<Path> stream = Files.list(path).sorted();
        for (Path child : Utils.streamIterator(stream)) {
            addPrePostPath(script, child, actionType);
        }
    }

    private void addPrePostScript(SQLScript script, Path fileName, SQLActionType actionType) throws IOException {
        try {
            String prePostScript = Files.readString(fileName);
            prePostScript = "-- " + fileName + "\n\n" + prePostScript; //$NON-NLS-1$ //$NON-NLS-2$
            script.addStatementWithoutSeparator(prePostScript, actionType);
        } catch (IOException e) {
            throw new IOException(Messages.PgDiff_read_error + e.getLocalizedMessage(), e);
        }
    }

    private String getMsScript(Set<ActionContainer> actions, Set<PgStatement> toRefresh, List<TreeElement> selected,
            AbstractDatabase oldDb, AbstractDatabase newDb) {
        SQLScript script = new SQLScript(settings);
        if (settings.isAddTransaction()) {
            script.addStatement("BEGIN TRANSACTION", SQLActionType.BEGIN); //$NON-NLS-1$
        }

        ActionsToScriptConverter.fillScript(script, actions, toRefresh, oldDb, newDb, selected);

        if (settings.isAddTransaction()) {
            script.addStatement("COMMIT", SQLActionType.END); //$NON-NLS-1$
        }

        return script.getFullScript();
    }

    private String getChScript(Set<ActionContainer> actions, Set<PgStatement> toRefresh, List<TreeElement> selected,
            AbstractDatabase oldDb, AbstractDatabase newDb) {
        SQLScript script = new SQLScript(settings);
        ActionsToScriptConverter.fillScript(script, actions, toRefresh, oldDb, newDb, selected);
        return script.getFullScript();
    }

    protected List<TreeElement> getSelectedElements(TreeElement root, IgnoreList ignoreList) {
        return new TreeFlattener()
                .onlySelected()
                .useIgnoreList(ignoreList)
                .onlyTypes(settings.getAllowedTypes())
                .flatten(root);
    }

    private Set<ActionContainer> resolveDependencies(List<TreeElement> selected,
                                                     AbstractDatabase oldDb,
                                                     AbstractDatabase newDb,
                                                     List<Entry<PgStatement, PgStatement>> additionalDependenciesOldDb,
                                                     List<Entry<PgStatement, PgStatement>> additionalDependenciesNewDb,
                                                     Set<PgStatement> toRefresh) {
        addColumnsAsElements(oldDb, newDb, selected);

        selected.sort(new CompareTree());

        List<DbObject> objects = new ArrayList<>();
        for (TreeElement st : selected) {
            PgStatement oldStatement = null;
            PgStatement newStatement = null;
            switch (st.getSide()) {
            case LEFT:
                oldStatement = st.getPgStatement(oldDb);
                break;
            case BOTH:
                oldStatement = st.getPgStatement(oldDb);
                newStatement = st.getPgStatement(newDb);
                break;
            case RIGHT:
                newStatement = st.getPgStatement(newDb);
                break;
            }
            objects.add(new DbObject(oldStatement, newStatement));
        }
        return DepcyResolver.resolve(oldDb, newDb,
                additionalDependenciesOldDb, additionalDependenciesNewDb, toRefresh, objects, settings);
    }

    @Deprecated
    private void addColumnsAsElements(AbstractDatabase oldDb, AbstractDatabase newDb,
            List<TreeElement> selected) {
        List<TreeElement> tempColumns = new ArrayList<>();
        for (TreeElement el : selected) {
            if (el.getType() == DbObjType.TABLE && el.getSide() == DiffSide.BOTH) {
                AbstractTable oldTbl = (AbstractTable) el.getPgStatement(oldDb);
                AbstractTable newTbl = (AbstractTable) el.getPgStatement(newDb);
                DiffTree.addColumns(oldTbl.getColumns(), newTbl.getColumns(), el, tempColumns);
            }
        }
        selected.addAll(tempColumns);
    }
}
