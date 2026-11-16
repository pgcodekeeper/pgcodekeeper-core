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

import org.jgrapht.Graph;
import org.jgrapht.alg.connectivity.GabowStrongConnectivityInspector;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.EdgeReversedGraph;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.database.pg.schema.*;
import org.pgcodekeeper.core.dependencieslist.Dependency;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Database dependency graph for managing object relationships and dependencies.
 * Builds directed graph of database objects with support for cycle detection and resolution.
 * Handles foreign key relationships, inheritance, and partitioning dependencies.
 */
public final class DepcyGraph {

    private static final Logger LOG = LoggerFactory.getLogger(DepcyGraph.class);

    private static final String REMOVE_DEP = Messages.DepcyGraph_log_remove_deps;

    private final Graph<IStatement, DefaultEdge> graph =
            new SimpleDirectedGraph<>(DefaultEdge.class);

    private final EdgeReversedGraph<IStatement, DefaultEdge> reversedGraph =
            new EdgeReversedGraph<>(graph);

    /**
     * Gets the dependency graph.
     * Graph direction: dependent object → dependency (source → target)
     *
     * @return the dependency graph
     */
    public Graph<IStatement, DefaultEdge> getGraph() {
        return graph;
    }

    public EdgeReversedGraph<IStatement, DefaultEdge> getReversedGraph() {
        return reversedGraph;
    }

    /**
     * Creates a dependency graph with optional graph reduction.
     *
     * @param graphSrc    the source database to build graph from
     * @param reduceGraph if true, merge column nodes into table nodes
     * @param additionalDependencies list of additional dependencies
     */
    public DepcyGraph(IDatabase graphSrc, boolean reduceGraph, Collection<Dependency> additionalDependencies) {
        create(graphSrc);
        removeCycles();

        if (reduceGraph) {
            reduce();
        }

        addCustomDepcies(additionalDependencies, graphSrc);
    }

    private void create(IDatabase db) {
        graph.addVertex(db);

        // first pass: object tree
        db.getDescendants().flatMap(ITable::columnAdder).forEach(st -> {
            graph.addVertex(st);
            graph.addEdge(st, st.getParent());
        });


        // second pass: dependency graph
        db.getDescendants().flatMap(ITable::columnAdder).forEach(st -> {
            for (ObjectReference dep : st.getDependencies()) {
                IStatement depSt = db.getStatement(dep);
                if (depSt != null && !st.equals(depSt)) {
                    graph.addEdge(st, depSt);
                }
            }

            if (st instanceof IConstraintFk fk) {
                createFkeyToUnique(fk, db);
            } else if (st instanceof PgColumn col) {
                AbstractStatement tbl = col.getParent();
                if (st.getParent() instanceof PgPartitionTable) {
                    createChildColToPartTblCol((PgPartitionTable) tbl, col, db);
                } else {
                    // Creating the connection between the column of a inherit
                    // table and the columns of its child tables.

                    IColumn parentTblCol = col.getParentCol((PgAbstractTable) tbl);
                    if (parentTblCol != null) {
                        graph.addEdge(col, parentTblCol);
                    }
                }
            }
        });
    }

    /**
     * The only way to find this depcy is to compare refcolumns against all existing unique
     * contraints/keys in reftable.
     * Unfortunately they might not exist at the stage where {@link AbstractStatement#getDependencies()}
     * are populated so we have to defer their lookup until here.
     * @param con foreign key
     * @param db database
     */
    private void createFkeyToUnique(IConstraintFk con, IDatabase db) {
        Collection<String> refs = con.getForeignColumns();
        if (refs.isEmpty()) {
            return;
        }

        IStatement cont = db.getStatement(
                new ObjectReference(con.getForeignSchema(), con.getForeignTable(), DbObjType.TABLE));

        if (cont instanceof IStatementContainer c) {
            for (IStatement refCon : c.getChildrenByType(DbObjType.CONSTRAINT)) {
                if (refCon instanceof IConstraintPk fkCon && refs.equals(fkCon.getColumns())) {
                    graph.addEdge(con, refCon);
                }
            }
            for (IStatement ref : c.getChildrenByType(DbObjType.INDEX)) {
                var refInd = (IIndex) ref;
                if (refInd.isUnique() && refInd.compareColumns(refs)) {
                    graph.addEdge(con, refInd);
                }
            }
        }
    }

    private void removeCycles() {
        // 1. Gabow's algorithm to find Strongly Connected Components (SCCs).
        // It runs in linear time O(V + E) and is highly memory-efficient.
        var inspector = new GabowStrongConnectivityInspector<>(graph);

        // 2. Fetch the components as lightweight sets of vertices.
        // This avoids creating heavy subgraph objects and minimizes object allocations.
        List<Set<IStatement>> sets = inspector.stronglyConnectedSets();

        // Reusable buffers — created once per removeCycles() call instead of on every
        // hasPathInComponent() invocation. This removes HashMap$HashIterator.<init> from the profile.
        // Identity-based sets/maps are used because JGraphT stores the very same vertex
        // objects in the graph and in componentVertices, so reference comparison is enough
        // and avoids the expensive AbstractStatement.equals.
        Set<IStatement> visitedBuf = Collections.newSetFromMap(new IdentityHashMap<>());
        Deque<IStatement> queueBuf = new ArrayDeque<>();

        // A temporary list to collect edges scheduled for removal.
        // This prevents ConcurrentModificationException while traversing the graph structures.
        List<DefaultEdge> edgesToRemove = new ArrayList<>();

        for (Set<IStatement> componentVertices : sets) {
            // If a component contains 1 or 0 vertices, it cannot form a cycle with other elements.
            if (componentVertices.size() <= 1) {
                continue;
            }

            // Identity-based view of the component vertices — contains() works by reference,
            // not via the expensive AbstractStatement.equals.
            Set<IStatement> componentIdent = Collections.newSetFromMap(new IdentityHashMap<>());
            componentIdent.addAll(componentVertices);

            // Iterate through the vertices belonging strictly to this "cyclic island".
            for (IStatement st : componentVertices) {
                if (!(st instanceof PgAbstractFunction)) {
                    continue;
                }

                // Fast pre-filter: the function must have at least one outgoing edge
                // to a COLUMN inside this same SCC. If there are none, hasPathInComponent()
                // is not worth calling.
                List<DefaultEdge> candidateEdges = null;
                for (DefaultEdge edge : graph.outgoingEdgesOf(st)) {
                    IStatement vertex = graph.getEdgeTarget(edge);
                    if (vertex.getStatementType() == DbObjType.COLUMN
                            && componentIdent.contains(vertex)) {
                        if (candidateEdges == null) {
                            candidateEdges = new ArrayList<>();
                        }
                        candidateEdges.add(edge);
                    }
                }

                if (candidateEdges == null) {
                    continue;
                }

                for (DefaultEdge edge : candidateEdges) {
                    IStatement vertex = graph.getEdgeTarget(edge);

                    // Verify that there is actually a path back from 'vertex' to 'st'
                    // inside the SCC (confirming this specific edge forms a cycle).
                    if (hasPathInComponent(vertex, st, componentIdent, visitedBuf, queueBuf)) {
                        edgesToRemove.add(edge);

                        // Check if the parent table is also involved in the same cycle.
                        IStatement table = vertex.getParent();
                        if (table != null) {
                            DefaultEdge tableEdge = graph.getEdge(st, table);
                            if (tableEdge != null) {
                                edgesToRemove.add(tableEdge);
                            }
                        }
                    }
                }
            }
        }

        // Remove the collected edges and log the actions.
        for (int i = 0; i < edgesToRemove.size(); i++) {
            DefaultEdge edge = edgesToRemove.get(i);
            IStatement source = graph.getEdgeSource(edge);
            IStatement target = graph.getEdgeTarget(edge);

            if (graph.removeEdge(edge)) {
                var msg = REMOVE_DEP.formatted(source.getQualifiedName(), target.getQualifiedName());
                LOG.info(msg);
            }
        }
    }

    /**
     * Lightweight BFS strictly bounded by the small strongly connected component
     * to check if 'target' (the function) is reachable from 'start' (the column).
     *
     * The visited/queue buffers are reused across calls — the caller is responsible
     * for clearing them before invoking this method (see removeCycles).
     */
    private boolean hasPathInComponent(IStatement start,
                                       IStatement target,
                                       Set<IStatement> componentIdent,
                                       Set<IStatement> visited,
                                       Deque<IStatement> queue) {
        visited.clear();
        queue.clear();

        queue.add(start);
        visited.add(start);

        while (!queue.isEmpty()) {
            IStatement current = queue.poll();
            for (DefaultEdge edge : graph.outgoingEdgesOf(current)) {
                IStatement next = graph.getEdgeTarget(edge);

                // Identity comparison: the graph holds the very same vertex objects as componentIdent.
                if (next == target) {
                    return true;
                }
                // Only traverse vertices belonging to this specific SCC.
                if (componentIdent.contains(next) && visited.add(next)) {
                    queue.add(next);
                }
            }
        }
        return false;
    }

    private void reduce() {
        List<Pair<IStatement, IStatement>> newEdges = new ArrayList<>();
        for (DefaultEdge edge : graph.edgeSet()) {
            var source = graph.getEdgeSource(edge);
            var target = graph.getEdgeTarget(edge);
            boolean changeEdge = false;
            if (source.getStatementType() == DbObjType.COLUMN) {
                changeEdge = true;
                source = source.getParent();
            }
            if (target.getStatementType() == DbObjType.COLUMN) {
                changeEdge = true;
                target = target.getParent();
            }
            if (changeEdge && !source.equals(target)) {
                newEdges.add(new Pair<>(source, target));
            }
        }
        for (var edge : newEdges) {
            graph.addEdge(edge.getFirst(), edge.getSecond());
        }

        List<IStatement> toRemove = new ArrayList<>();
        for (var st : graph.vertexSet()) {
            if (st.getStatementType() == DbObjType.COLUMN) {
                toRemove.add(st);
            }
        }
        graph.removeAllVertices(toRemove);
    }

    /**
     * Creates the connection between the column of a partitioned table and the
     * columns of its sections (child tables).
     * <br />
     * Partitioned tables cannot use the inheritance mechanism, as in simple tables.
     * @param db database
     */
    private void createChildColToPartTblCol(PgPartitionTable tbl, PgColumn col, IDatabase db) {
        for (Inherits in : tbl.getInherits()) {
            IStatement parentTbl = db.getStatement(new ObjectReference(in.key(), in.value(), DbObjType.TABLE));
            if (parentTbl == null) {
                var msg = Messages.DepcyGraph_log_no_such_table.formatted(in.getQualifiedName());
                LOG.error(msg);
                continue;
            }

            if (parentTbl instanceof PgPartitionTable partTable) {
                createChildColToPartTblCol(partTable, col, db);
            } else {
                String colName = col.getName();
                IColumn parentCol = ((ITable) parentTbl).getColumn(colName);
                if (parentCol != null) {
                    graph.addEdge(col, parentCol);
                } else {
                    var msg = Messages.DepcyGraph_log_col_is_missed.formatted(
                            in.getQualifiedName(), colName, col.getSchemaName(), col.getParent().getName(), colName
                    );
                    LOG.error(msg);
                }
            }
        }
    }

    /**
     * Adds custom dependencies to the graph.
     *
     * @param dependencies list of custom dependency pairs to add
     * @param db database
     */
    private void addCustomDepcies(Collection<Dependency> dependencies, IDatabase db) {
        if (dependencies == null) {
            return;
        }
        for (var dep : dependencies) {
            IStatement source = db.getStatement(dep.source());
            IStatement target = db.getStatement(dep.target());

            if (source != null && target != null) {
                graph.addEdge(source, target);
            }
        }
    }
}
