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
package org.pgcodekeeper.core.model.graph;

import org.jgrapht.graph.DefaultEdge;
import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.ms.schema.MsSourceStatement;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.database.ms.schema.MsAbstractFunction;
import org.pgcodekeeper.core.database.ms.schema.MsTable;
import org.pgcodekeeper.core.database.ms.schema.MsType;
import org.pgcodekeeper.core.database.ms.schema.MsView;
import org.pgcodekeeper.core.database.pg.schema.PgIndex;
import org.pgcodekeeper.core.database.pg.schema.PgSequence;
import org.pgcodekeeper.core.database.pg.schema.PgTypedTable;
import org.pgcodekeeper.core.script.SQLScript;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.*;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.IntStream;

/**
 * Core dependency resolution engine that determines database object changes required for schema migration.
 * Analyzes two database schemas and generates a complete set of CREATE, ALTER, and DROP actions
 * while respecting object dependencies and handling complex dependency chains.
 * <p>
 * Implementation notes:
 * <p>
 * General idea behind this class is graph passes that collect required actions.
 * addDropStatements starts a bottom-to-top pass in the old DB graph,
 * addCreateStatements starts a top-to-bottom pass in the new DB graph.
 * When these passes reach an object requiring an ALTER,
 * an "opposite direction" pass for that object is started.
 * This also allows us to treat alters as "drops" here.
 * Passes are eventually exhausted when all the actions have been collected
 * into actions set.
 * <p>
 * At the very end recreateDrops is called, which starts a "create pass"
 * for every object that was dropped but should not have been -
 * i.e. it was a dependency related drop. These passes are performed until
 * they stop generating new actions. This ensures that all dropped dependencies
 * have been recreated, and any dependency drops that may have been generated in the process
 * have also been accounted for.
 */
public final class DepcyResolver {

    private final IDatabase oldDb;
    private final IDatabase newDb;
    private final DepcyGraph oldDepcyGraph;
    private final DepcyGraph newDepcyGraph;
    private final Set<IStatement> toRefresh;

    private final Set<ActionContainer> actions = new LinkedHashSet<>();
    /**
     * Stores objects that have been processed for drop operations
     */
    private final Set<IStatement> droppedObjects = new HashSet<>();
    private final Set<IStatement> triedToDrop = new HashSet<>();

    /**
     * Stores objects that have been processed for create operations
     */
    private final Set<IStatement> createdObjects = new HashSet<>();

    /**
     * Stores the result of method appendAlterSQL. Key - {@link IStatement}, value - {@link ObjectState}
     */
    private final Map<IStatement, ObjectState> states = new HashMap<>();

    private final Map<String, Boolean> recreatedObjs = new HashMap<>();

    private final ISettings settings;

    public DepcyResolver(IDatabase oldDatabase, IDatabase newDatabase, ISettings settings, Set<IStatement> toRefresh) {
        this.oldDb = oldDatabase;
        this.newDb = newDatabase;
        this.oldDepcyGraph = new DepcyGraph(oldDatabase);
        this.newDepcyGraph = new DepcyGraph(newDatabase);
        this.toRefresh = toRefresh;
        this.settings = settings;
    }

    private void fillObjects(List<DbObject> objects) {
        for (DbObject obj : objects) {
            if (obj.newStatement() == null) {
                addDropStatements(obj.oldStatement(), null);
            } else if (obj.oldStatement() == null) {
                addCreateStatements(obj.newStatement(), null);
            } else {
                addAlterStatements(obj.oldStatement(), obj.newStatement());
            }
        }
    }

    /**
     * Processes creation of an object in the new database by adding all required dependencies.
     * When an object exists in the new database but not in the old, this method initiates
     * its creation along with all dependencies required for proper operation.
     *
     * @param newStatement the object in the new database to be created
     * @param starter      the object that initiated this creation process
     */
    private void addCreateStatements(IStatement newStatement, IStatement starter) {
        if (!createdObjects.add(newStatement)) {
            return;
        }

        for (var dependency : GraphUtils.forward(newDepcyGraph, newStatement)) {
            tryToCreate(dependency, newStatement);
        }
        tryToCreate(newStatement, starter);
    }

    /**
     * Processes deletion of an object from the old database by adding all dependent objects for removal.
     * When an object exists in the old database but not in the new, this method initiates
     * its deletion along with all objects that depend on it, as they would be invalid without it.
     *
     * @param oldStatement the object in the old database to be deleted
     * @param starter      the object that initiated this deletion process
     */
    private void addDropStatements(IStatement oldStatement, IStatement starter) {
        if (!droppedObjects.add(oldStatement)) {
            return;
        }

        for (var dependent : GraphUtils.reverse(oldDepcyGraph, oldStatement)) {
            tryToDrop(dependent, oldStatement);
        }

        tryToDrop(oldStatement, starter);
        resolveCannotDrop(oldStatement);
    }

    private void resolveCannotDrop(IStatement oldStatement) {
        if (oldStatement.canDrop() || oldStatement.getParent().getTwin(newDb) == null) {
            return;
        }

        for (var dep : GraphUtils.forward(oldDepcyGraph, oldStatement)) {
            if (dep instanceof PgIndex) {
                addToListWithoutDepcies(ObjectState.DROP, dep, oldStatement);
                addDropStatements(dep, oldStatement);
            }
        }
    }

    /**
     * Adds statements for altering a database object.
     * Determines the appropriate action based on object state comparison
     * and handles dependency-related recreations when necessary.
     *
     * @param oldStatement the original object state
     * @param newStatement the target object state
     */
    private void addAlterStatements(IStatement oldStatement, IStatement newStatement) {
        ObjectState state = getObjectState(oldStatement, newStatement);
        if (state.in(ObjectState.RECREATE, ObjectState.ALTER_WITH_DEP)) {
            addDropStatements(oldStatement, null);
            return;
        }

        // add altered objects
        // skip table columns from drop list
        if (state == ObjectState.ALTER && !inDropsList(oldStatement)
                && (oldStatement.getStatementType() != DbObjType.COLUMN || !inDropsList(oldStatement.getParent()))) {
            addToListWithoutDepcies(ObjectState.ALTER, oldStatement, null);
        }

        alterMsTableColumns(oldStatement, newStatement);
    }

    private void alterMsTableColumns(IStatement oldStatement, IStatement newStatement) {
        // if no depcies were triggered for a MsTable alter
        // check for column layout changes and refresh views
        if (oldStatement instanceof MsTable tOld && newStatement instanceof MsTable tNew) {
            List<IColumn> cOld = tOld.getColumns();
            List<IColumn> cNew = tNew.getColumns();

            // first check for columns added or removed
            boolean colLayoutChanged = cOld.size() != cNew.size();
            if (!colLayoutChanged) {
                // second, columns replaced or reordered
                for (int i = 0; i < cOld.size(); ++i) {
                    if (!cOld.get(i).getName().equals(cNew.get(i).getName())) {
                        colLayoutChanged = true;
                        break;
                    }
                }
            }

            if (colLayoutChanged) {
                refreshDependents(tOld);
            }
        }
    }

    private void refreshDependents(AbstractStatement oldStatement) {
        for (var dependent : GraphUtils.reverse(oldDepcyGraph, oldStatement)) {
            if (dependent instanceof MsView && dependent.getTwin(newDb) != null) {
                toRefresh.add(dependent);
            }
        }
    }

    private void removeAlteredFromRefreshes() {
        toRefresh.removeIf(st ->
                actions.stream().anyMatch(action -> action.getState() == ObjectState.ALTER
                        && action.getOldObj() instanceof MsView
                        && action.getOldObj().equals(st))
        );
    }

    /**
     * Recreates previously dropped objects into their new state.
     * Handles cases where objects were dropped due to dependencies but should
     * actually exist in the target schema. Continues until no new actions are generated.
     */
    private void recreateDrops() {
        int oldActionsSize = -1;
        List<IStatement> toRecreate = new ArrayList<>();
        // since a recreate can trigger a drop via  dependency being altered
        // run recreates until no more statements are being added (may need optimization)
        while (actions.size() > oldActionsSize) {
            toRecreate.clear();
            oldActionsSize = actions.size();
            for (ActionContainer action : actions) {
                if (action.getState() == ObjectState.DROP) {
                    toRecreate.add(action.getOldObj());
                }
            }
            for (IStatement drop : toRecreate) {
                var newSt = drop.getTwin(newDb);
                if (newSt != null) {
                    // add views to emit refreshes others are to block drop+create pairs for unchanged statements
                    fillRefresh(drop, newSt);
                    addCreateStatements(newSt, null);
                }
            }
        }
    }

    private void fillRefresh(IStatement drop, IStatement newSt) {
        if (newSt instanceof MsSourceStatement) {
            if (newSt instanceof MsAbstractFunction && isMsTypeDep(newSt)) {
                return;
            }

            if (newSt instanceof MsView view && view.isSchemaBinding()) {
                return;
            }

            if (newSt.equals(drop) && !inDropsList(newSt.getParent())) {
                toRefresh.add(newSt);
            }
        }
    }

    // check if obj dependence of ms Type
    private boolean isMsTypeDep(IStatement newSt) {
        var graph = newDepcyGraph.getGraph();
        for (DefaultEdge edge : graph.edgeSet()) {
            var source = graph.getEdgeSource(edge);
            if (newSt.equals(source)) {
                var target = graph.getEdgeTarget(edge);
                if (target instanceof MsType && inDropsList(target)) {
                    return true;
                }
            }
        }
        return false;
    }

    private void tryToDrop(IStatement oldObj, IStatement starter) {
        if (!triedToDrop.add(oldObj)) {
            return;
        }

        // Initially set action to drop the object
        ObjectState action = ObjectState.DROP;
        if (!oldObj.canDrop()) {
            addToListWithoutDepcies(action, oldObj, starter);
            return;
        }

        var newObj = oldObj.getTwin(newDb);
        if (newObj != null && !hasDroppedDependency(oldObj)) {
            action = getObjectState(oldObj, newObj);
            if (action == ObjectState.NOTHING) {
                return;
            }

            // when altering an object with dependencies,
            // first create the object with dependencies,
            // then alter it
            if (action == ObjectState.ALTER_WITH_DEP) {
                // do not add object if already in the list
                if (!createdObjects.contains(newObj)) {
                    addCreateStatements(newObj, null);
                    addToListWithoutDepcies(action, oldObj, starter);
                }
                return;
            }

            if (action == ObjectState.RECREATE) {
                action = ObjectState.DROP;
            }
        }

        // Columns are skipped when dropping the table
        if (oldObj.getStatementType() == DbObjType.COLUMN) {
            ITable oldTable = (ITable) oldObj.getParent();
            var newTable = oldObj.getParent().getTwin(newDb);

            if (newTable == null || getRecreatedObj(oldTable, (ITable) newTable)) {
                // case where dependency drop affects a column we don't handle
                // because the table is being dropped - drop the table instead
                addDropStatements(oldTable, oldObj);
                return;
            }

            // also skip during recreate
            ObjectState parentState = getObjectState(oldTable, newTable);
            if (parentState == ObjectState.RECREATE) {
                return;
            }

            if (isColumnChangeOverlap(oldTable, newTable)) {
                return;
            }
        }

        // skip sequence if its owned-by column is being dropped
        // sequence will be dropped implicitly with the column
        if (oldObj instanceof PgSequence seq) {
            GenericColumn ownedBy = seq.getOwnedBy();
            if (ownedBy != null && newDb.getStatement(ownedBy) == null) {
                return;
            }
        }

        addToListWithoutDepcies(action, oldObj, starter);
    }

    private boolean hasDroppedDependency(IStatement oldState) {
        for (var dependency : GraphUtils.forward(oldDepcyGraph, oldState)) {
            DbObjType type = dependency.getStatementType();
            var newSt = dependency.getTwin(newDb);
            if (newSt == null) {
                if (type == DbObjType.FUNCTION && isDefaultsOnlyChange((IFunction) dependency)) {
                    // when function's signature changes it has no twin
                    // but the dependent object might be unchanged
                    // due to default arguments changing in the signature
                    return true;
                }
                continue;
            }

            if (type.in(DbObjType.FUNCTION, DbObjType.PROCEDURE)
                    && !((IFunction) dependency).needDrop((IFunction) newSt)) {
                continue;
            }

            ObjectState state = getObjectState(dependency, newSt);
            if (state.in(ObjectState.RECREATE, ObjectState.ALTER_WITH_DEP)) {
                return true;
            }
        }

        return false;
    }

    private boolean isDefaultsOnlyChange(IFunction oldFunc) {
        ISchema newSchema = newDb.getSchema(oldFunc.getSchemaName());
        if (newSchema == null) {
            return false;
        }

        // in the new database, search the function for which
        // the signature before first default argument will be the same
        // if there is such, then the drop is necessary,
        // if there is no such, then the drop is not necessary
        Function<IFunction, List<? extends IArgument>> argsBeforeDefaults = f -> {
            var args = f.getArguments();
            OptionalInt firstDefault = IntStream.range(0, args.size())
                    .filter(i -> args.get(i).getDefaultExpression() != null)
                    .findFirst();
            return firstDefault.isPresent() ? args.subList(0, firstDefault.getAsInt()) : args;
        };

        var oldArgs = argsBeforeDefaults.apply(oldFunc);

        var allFuncs = newSchema.getChildrenByType(DbObjType.FUNCTION);

        return allFuncs.stream()
                .map(IFunction.class::cast)
                .filter(f -> oldFunc.getBareName().equals(f.getBareName()))
                .map(argsBeforeDefaults)
                .anyMatch(oldArgs::equals);
    }

    private boolean isColumnChangeOverlap(IStatement oldTable, IStatement newTable) {
        // skip columns if table type changed
        if (!oldTable.getClass().equals(newTable.getClass())) {
            return true;
        }

        // columns are integrated into CREATE TABLE OF TYPE
        if (newTable instanceof PgTypedTable newTypedTable) {
            PgTypedTable oldTypedTable = (PgTypedTable) oldTable;
            return !Objects.equals(newTypedTable.getOfType(), oldTypedTable.getOfType());
        }

        return false;
    }

    /**
     * Removes actions that for some reason should not be included in the script
     */
    private void removeExtraActions() {
        Set<ActionContainer> toRemove = new HashSet<>();
        for (ActionContainer action : actions) {
            if (action.getState() != ObjectState.ALTER) {
                continue;
            }
            // case where the selected modified object was recreated due to a dependency
            var newObj = action.getNewObj();
            if (actions.contains(new ActionContainer(newObj, newObj, ObjectState.CREATE, null))) {
                toRemove.add(action);
            }
        }
        actions.removeAll(toRemove);
    }

    /**
     * Checks if an object exists in the list of previously dropped objects.
     *
     * @param statement the object to check
     * @return true if the object is in the drops list, false otherwise
     */
    private boolean inDropsList(IStatement statement) {
        // if owned-by column or table is already in drop list
        // then sequence will also be dropped implicitly, return true
        if (statement instanceof PgSequence seq) {
            GenericColumn ownedBy = seq.getOwnedBy();
            if (ownedBy != null) {
                var column = oldDb.getStatement(ownedBy);
                return column != null && (inDropsList(column) || inDropsList(column.getParent()));
            }
        }

        IStatement oldObj = statement.getTwin(oldDb);
        return actions.contains(new ActionContainer(oldObj, oldObj, ObjectState.DROP, null));
    }

    /**
     * Adds an action to the script expressions list without processing dependencies.
     *
     * @param action  the action type to perform (see {@link ObjectState})
     * @param oldObj  the object from the old database state
     * @param starter the object that triggered this action
     */
    private void addToListWithoutDepcies(ObjectState action,
                                         IStatement oldObj, IStatement starter) {
        switch (action) {
            case CREATE, DROP -> actions.add(new ActionContainer(oldObj, oldObj, action, starter));
            case ALTER, ALTER_WITH_DEP -> actions
                    .add(new ActionContainer(oldObj, oldObj.getTwin(newDb), ObjectState.ALTER, starter));
            default -> throw new IllegalStateException("Not implemented action");
        }
    }

    private void tryToCreate(IStatement newObj, IStatement starter) {
        // Initially set action to create the object
        ObjectState action = ObjectState.CREATE;
        // always create if droppped before
        if (inDropsList(newObj)) {
            createColumnDependencies(newObj);
            addToListWithoutDepcies(action, newObj, null);
            return;
        }

        if (newObj.getStatementType() == DbObjType.COLUMN) {
            var oldTable = newObj.getParent().getTwin(oldDb);
            ITable newTable = (ITable) newObj.getParent();
            if (oldTable == null || getRecreatedObj((ITable) oldTable, newTable)) {
                // columns are integrated into CREATE TABLE
                return;
            }

            if (isColumnChangeOverlap(oldTable, newTable)) {
                return;
            }
        }

        var oldObj = newObj.getTwin(oldDb);
        if (oldObj != null) {
            action = getObjectState(oldObj, newObj);
            if (action == ObjectState.NOTHING) {
                return;
            }

            // when altering object with dependencies
            if (action.in(ObjectState.RECREATE, ObjectState.ALTER_WITH_DEP)) {
                addDropStatements(oldObj, starter);
                if (action == ObjectState.ALTER_WITH_DEP) {
                    // add alter for old object
                    addToListWithoutDepcies(action, oldObj, starter);
                    return;
                }
                action = ObjectState.CREATE;
            }
        }

        // if object (table) is being created, initiate creation of its column dependencies
        // columns themselves will be created implicitly with the table
        if (action == ObjectState.CREATE) {
            createColumnDependencies(newObj);
        }

        // create column when creating sequence with owned-by relationship
        if (newObj instanceof PgSequence seq) {
            GenericColumn ownedBy = seq.getOwnedBy();
            if (ownedBy != null && oldDb.getStatement(ownedBy) == null) {
                var col = newDb.getStatement(ownedBy);
                if (col != null) {
                    addCreateStatements(col, newObj);
                }
            }
        }

        addToListWithoutDepcies(action, newObj, starter);
    }

    private void createColumnDependencies(IStatement newObj) {
        if (newObj instanceof ITable table) {
            // create column dependencies before table
            for (IColumn col : table.getColumns()) {
                addCreateStatements(col, null);
            }
        }
    }

    private ObjectState getObjectState(IStatement oldSt, IStatement newSt) {
        return states.computeIfAbsent(oldSt, x -> oldSt.appendAlterSQL(newSt, new SQLScript(settings, newSt.getSeparator())));
    }

    private Boolean getRecreatedObj(ITable oldTable, ITable newTable) {
        return recreatedObjs.computeIfAbsent(oldTable.getQualifiedName(), x -> oldTable.isRecreated(newTable, settings));
    }

    public static Set<ActionContainer> resolve(IDatabase oldDb,
                                               IDatabase newDb,
                                               List<Entry<IStatement, IStatement>> additionalDependenciesOldDb,
                                               List<Entry<IStatement, IStatement>> additionalDependenciesNewDb,
                                               Set<IStatement> toRefresh,
                                               List<DbObject> dbObjects,
                                               ISettings settings) {
        DepcyResolver depRes = new DepcyResolver(oldDb, newDb, settings, toRefresh);
        depRes.oldDepcyGraph.addCustomDepcies(additionalDependenciesOldDb);
        depRes.newDepcyGraph.addCustomDepcies(additionalDependenciesNewDb);
        depRes.fillObjects(dbObjects);
        depRes.recreateDrops();
        depRes.removeExtraActions();
        depRes.removeAlteredFromRefreshes();

        return depRes.actions;
    }
}
