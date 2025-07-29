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
 *******************************************************************************/
package org.pgcodekeeper.core.model.graph;

import org.jgrapht.graph.DefaultEdge;
import org.pgcodekeeper.core.DatabaseType;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.schema.*;
import org.pgcodekeeper.core.schema.ms.AbstractMsFunction;
import org.pgcodekeeper.core.schema.ms.MsTable;
import org.pgcodekeeper.core.schema.ms.MsType;
import org.pgcodekeeper.core.schema.ms.MsView;
import org.pgcodekeeper.core.schema.pg.PgIndex;
import org.pgcodekeeper.core.schema.pg.PgSequence;
import org.pgcodekeeper.core.schema.pg.TypedPgTable;
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

    private final AbstractDatabase oldDb;
    private final AbstractDatabase newDb;
    private final DepcyGraph oldDepcyGraph;
    private final DepcyGraph newDepcyGraph;
    private final Set<PgStatement> toRefresh;

    private final Set<ActionContainer> actions = new LinkedHashSet<>();
    /**
     * Stores objects that have been processed for drop operations
     */
    private final Set<PgStatement> droppedObjects = new HashSet<>();
    private final Set<PgStatement> triedToDrop = new HashSet<>();

    /**
     * Stores objects that have been processed for create operations
     */
    private final Set<PgStatement> createdObjects = new HashSet<>();

    /**
     * Stores the result of method appendAlterSQL. Key - {@link PgStatement}, value - {@link ObjectState}
     */
    private final Map<PgStatement, ObjectState> states = new HashMap<>();

    private final Map<String, Boolean> recreatedObjs = new HashMap<>();

    private final ISettings settings;

    public DepcyResolver(AbstractDatabase oldDatabase, AbstractDatabase newDatabase, ISettings settings, Set<PgStatement> toRefresh) {
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
    private void addCreateStatements(PgStatement newStatement, PgStatement starter) {
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
    private void addDropStatements(PgStatement oldStatement, PgStatement starter) {
        if (!droppedObjects.add(oldStatement)) {
            return;
        }

        for (var dependent : GraphUtils.reverse(oldDepcyGraph, oldStatement)) {
            tryToDrop(dependent, oldStatement);
        }

        tryToDrop(oldStatement, starter);
        resolveCannotDrop(oldStatement);
    }

    private void resolveCannotDrop(PgStatement oldStatement) {
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
    private void addAlterStatements(PgStatement oldStatement, PgStatement newStatement) {
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

    private void alterMsTableColumns(PgStatement oldStatement, PgStatement newStatement) {
        // if no depcies were triggered for a MsTable alter
        // check for column layout changes and refresh views
        if (oldStatement instanceof MsTable tOld && newStatement instanceof MsTable tNew) {
            List<AbstractColumn> cOld = tOld.getColumns();
            List<AbstractColumn> cNew = tNew.getColumns();

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

    private void refreshDependents(PgStatement oldStatement) {
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
        List<PgStatement> toRecreate = new ArrayList<>();
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
            for (PgStatement drop : toRecreate) {
                PgStatement newSt = drop.getTwin(newDb);
                if (newSt != null) {
                    // add views to emit refreshes others are to block drop+create pairs for unchanged statements
                    fillRefresh(drop, newSt);
                    addCreateStatements(newSt, null);
                }
            }
        }
    }

    private void fillRefresh(PgStatement drop, PgStatement newSt) {
        if (newSt instanceof SourceStatement) {
            if (newSt instanceof AbstractMsFunction && isMsTypeDep(newSt)) {
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
    private boolean isMsTypeDep(PgStatement newSt) {
        var graph = newDepcyGraph.getGraph();
        for (DefaultEdge edge : graph.edgeSet()) {
            PgStatement source = graph.getEdgeSource(edge);
            if (newSt.equals(source)) {
                PgStatement target = graph.getEdgeTarget(edge);
                if (target instanceof MsType && inDropsList(target)) {
                    return true;
                }
            }
        }
        return false;
    }

    private void tryToDrop(PgStatement oldObj, PgStatement starter) {
        if (!triedToDrop.add(oldObj)) {
            return;
        }

        // Initially set action to drop the object
        ObjectState action = ObjectState.DROP;
        if (!oldObj.canDrop()) {
            addToListWithoutDepcies(action, oldObj, starter);
            return;
        }

        PgStatement newObj = oldObj.getTwin(newDb);
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
            AbstractTable oldTable = (AbstractTable) oldObj.getParent();
            PgStatement newTable = oldObj.getParent().getTwin(newDb);

            if (newTable == null || getRecreatedObj(oldTable, (AbstractTable) newTable)) {
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

    private boolean hasDroppedDependency(PgStatement oldState) {
        for (var dependency : GraphUtils.forward(oldDepcyGraph, oldState)) {
            DbObjType type = dependency.getStatementType();
            PgStatement newSt = dependency.getTwin(newDb);
            if (newSt == null) {
                if (type == DbObjType.FUNCTION && dependency.getDbType() == DatabaseType.PG
                        && isDefaultsOnlyChange((IFunction) dependency)) {
                    // when function's signature changes it has no twin
                    // but the dependent object might be unchanged
                    // due to default arguments changing in the signature
                    return true;
                }
                continue;
            }

            if (type.in(DbObjType.FUNCTION, DbObjType.PROCEDURE)
                    && dependency.getDbType() != DatabaseType.CH
                    && !((AbstractFunction) dependency).needDrop((AbstractFunction) newSt)) {
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
        AbstractSchema newSchema = newDb.getSchema(oldFunc.getSchemaName());
        if (newSchema == null) {
            return false;
        }

        // in the new database, search the function for which
        // the signature before first default argument will be the same
        // if there is such, then the drop is necessary,
        // if there is no such, then the drop is not necessary
        Function<IFunction, List<Argument>> argsBeforeDefaults = f -> {
            List<Argument> args = f.getArguments();
            OptionalInt firstDefault = IntStream.range(0, args.size())
                    .filter(i -> args.get(i).getDefaultExpression() != null)
                    .findFirst();
            return firstDefault.isPresent() ? args.subList(0, firstDefault.getAsInt()) : args;
        };

        List<Argument> oldArgs = argsBeforeDefaults.apply(oldFunc);

        return newSchema.getFunctions().stream()
                .filter(f -> oldFunc.getBareName().equals(f.getBareName()))
                .map(argsBeforeDefaults)
                .anyMatch(oldArgs::equals);
    }

    private boolean isColumnChangeOverlap(PgStatement oldTable, PgStatement newTable) {
        // skip columns if table type changed
        if (!oldTable.getClass().equals(newTable.getClass())) {
            return true;
        }

        // columns are integrated into CREATE TABLE OF TYPE
        if (newTable instanceof TypedPgTable newTypedTable) {
            TypedPgTable oldTypedTable = (TypedPgTable) oldTable;
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
            PgStatement newObj = action.getNewObj();
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
    private boolean inDropsList(PgStatement statement) {
        // if owned-by column or table is already in drop list
        // then sequence will also be dropped implicitly, return true
        if (statement instanceof PgSequence seq) {
            GenericColumn ownedBy = seq.getOwnedBy();
            if (ownedBy != null) {
                PgStatement column = oldDb.getStatement(ownedBy);
                return column != null && (inDropsList(column) || inDropsList(column.getParent()));
            }
        }

        PgStatement oldObj = statement.getTwin(oldDb);
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
                                         PgStatement oldObj, PgStatement starter) {
        switch (action) {
            case CREATE, DROP -> actions.add(new ActionContainer(oldObj, oldObj, action, starter));
            case ALTER, ALTER_WITH_DEP -> actions
                    .add(new ActionContainer(oldObj, oldObj.getTwin(newDb), ObjectState.ALTER, starter));
            default -> throw new IllegalStateException("Not implemented action");
        }
    }

    private void tryToCreate(PgStatement newObj, PgStatement starter) {
        // Initially set action to create the object
        ObjectState action = ObjectState.CREATE;
        // always create if droppped before
        if (inDropsList(newObj)) {
            createColumnDependencies(newObj);
            addToListWithoutDepcies(action, newObj, null);
            return;
        }

        if (newObj.getStatementType() == DbObjType.COLUMN) {
            PgStatement oldTable = newObj.getParent().getTwin(oldDb);
            AbstractTable newTable = (AbstractTable) newObj.getParent();
            if (oldTable == null || getRecreatedObj((AbstractTable) oldTable, newTable)) {
                // columns are integrated into CREATE TABLE
                return;
            }

            if (isColumnChangeOverlap(oldTable, newTable)) {
                return;
            }
        }

        PgStatement oldObj = newObj.getTwin(oldDb);
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
                PgStatement col = newDb.getStatement(ownedBy);
                if (col != null) {
                    addCreateStatements(col, newObj);
                }
            }
        }

        addToListWithoutDepcies(action, newObj, starter);
    }

    private void createColumnDependencies(PgStatement newObj) {
        if (newObj.getStatementType() == DbObjType.TABLE) {
            // create column dependencies before table
            for (AbstractColumn col : ((AbstractTable) newObj).getColumns()) {
                addCreateStatements(col, null);
            }
        }
    }

    private ObjectState getObjectState(PgStatement oldSt, PgStatement newSt) {
        return states.computeIfAbsent(oldSt, x -> oldSt.appendAlterSQL(newSt, new SQLScript(settings)));
    }

    private Boolean getRecreatedObj(AbstractTable oldTable, AbstractTable newTable) {
        return recreatedObjs.computeIfAbsent(oldTable.getQualifiedName(), x -> oldTable.isRecreated(newTable, settings));
    }

    public static Set<ActionContainer> resolve(AbstractDatabase oldDb, AbstractDatabase newDb,
                                               List<Entry<PgStatement, PgStatement>> additionalDepciesOldDb,
                                               List<Entry<PgStatement, PgStatement>> additionalDepciesNewDb,
                                               Set<PgStatement> toRefresh, List<DbObject> dbObjects, ISettings settings) {
        DepcyResolver depRes = new DepcyResolver(oldDb, newDb, settings, toRefresh);
        depRes.oldDepcyGraph.addCustomDepcies(additionalDepciesOldDb);
        depRes.newDepcyGraph.addCustomDepcies(additionalDepciesNewDb);
        depRes.fillObjects(dbObjects);
        depRes.recreateDrops();
        depRes.removeExtraActions();
        depRes.removeAlteredFromRefreshes();

        return depRes.actions;
    }
}
