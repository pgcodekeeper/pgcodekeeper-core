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

import org.jgrapht.Graph;
import org.jgrapht.alg.cycle.CycleDetector;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.EdgeReversedGraph;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.pgcodekeeper.core.DatabaseType;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.schema.*;
import org.pgcodekeeper.core.schema.pg.AbstractPgFunction;
import org.pgcodekeeper.core.schema.pg.AbstractPgTable;
import org.pgcodekeeper.core.schema.pg.PartitionPgTable;
import org.pgcodekeeper.core.schema.pg.PgColumn;
import org.pgcodekeeper.core.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

/**
 * Database dependency graph for managing object relationships and dependencies.
 * Builds directed graph of database objects with support for cycle detection and resolution.
 * Handles foreign key relationships, inheritance, and partitioning dependencies.
 */
public final class DepcyGraph {

    private static final Logger LOG = LoggerFactory.getLogger(DepcyGraph.class);

    private static final String REMOVE_DEP = Messages.DepcyGraph_log_remove_deps;

    private final Graph<PgStatement, DefaultEdge> graph =
            new SimpleDirectedGraph<>(DefaultEdge.class);

    private final EdgeReversedGraph<PgStatement, DefaultEdge> reversedGraph =
            new EdgeReversedGraph<>(graph);

    /**
     * Gets the dependency graph.
     * Graph direction: dependent object → dependency (source → target)
     *
     * @return the dependency graph
     */
    public Graph<PgStatement, DefaultEdge> getGraph() {
        return graph;
    }

    public EdgeReversedGraph<PgStatement, DefaultEdge> getReversedGraph() {
        return reversedGraph;
    }

    private final AbstractDatabase db;

    /**
     * Copied database, graph source.<br>
     * <b>Do not modify</b> any elements in this as it will break
     * HashSets/HashMaps and with them the generated graph.
     */
    public AbstractDatabase getDb() {
        return db;
    }

    /**
     * Creates a dependency graph from the database schema.
     *
     * @param graphSrc the source database to build graph from
     */
    public DepcyGraph(AbstractDatabase graphSrc) {
        this(graphSrc, false);
    }

    /**
     * Creates a dependency graph with optional graph reduction.
     *
     * @param graphSrc    the source database to build graph from
     * @param reduceGraph if true, merge column nodes into table nodes
     */
    public DepcyGraph(AbstractDatabase graphSrc, boolean reduceGraph) {
        db = (AbstractDatabase) graphSrc.deepCopy();
        create();
        removeCycles();

        if (reduceGraph) {
            reduce();
        }
    }

    private void create() {
        graph.addVertex(db);

        // first pass: object tree
        db.getDescendants().flatMap(AbstractTable::columnAdder).forEach(st -> {
            graph.addVertex(st);
            graph.addEdge(st, st.getParent());
        });


        // second pass: dependency graph
        db.getDescendants().flatMap(AbstractTable::columnAdder).forEach(st -> {
            processDeps(st);
            if (st instanceof IConstraintFk fk) {
                createFkeyToUnique(fk);
            } else if (st.getStatementType() == DbObjType.COLUMN
                    && st.getDbType() == DatabaseType.PG) {
                PgColumn col = (PgColumn) st;
                PgStatement tbl = col.getParent();
                if (st.getParent() instanceof PartitionPgTable) {
                    createChildColToPartTblCol((PartitionPgTable) tbl, col);
                } else {
                    // Creating the connection between the column of a inherit
                    // table and the columns of its child tables.

                    AbstractColumn parentTblCol = col.getParentCol((AbstractPgTable) tbl);
                    if (parentTblCol != null) {
                        graph.addEdge(col, parentTblCol);
                    }
                }
            }
        });
    }

    private void reduce() {
        List<Pair<PgStatement, PgStatement>> newEdges = new ArrayList<>();
        for (DefaultEdge edge : graph.edgeSet()) {
            PgStatement source = graph.getEdgeSource(edge);
            PgStatement target = graph.getEdgeTarget(edge);
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
        for (Pair<PgStatement, PgStatement> edge : newEdges) {
            graph.addEdge(edge.getFirst(), edge.getSecond());
        }

        List<PgStatement> toRemove = new ArrayList<>();
        for (PgStatement st : graph.vertexSet()) {
            if (st.getStatementType() == DbObjType.COLUMN) {
                toRemove.add(st);
            }
        }
        graph.removeAllVertices(toRemove);
    }

    private void removeCycles() {
        CycleDetector<PgStatement, DefaultEdge> detector = new CycleDetector<>(graph);

        for (PgStatement st : detector.findCycles()) {
            if (!(st instanceof AbstractPgFunction)) {
                continue;
            }

            for (PgStatement vertex : detector.findCyclesContainingVertex(st)) {
                if (vertex.getStatementType() == DbObjType.COLUMN) {
                    graph.removeEdge(st, vertex);
                    var msg = REMOVE_DEP.formatted(st.getQualifiedName(), vertex.getQualifiedName());
                    LOG.info(msg);

                    PgStatement table = vertex.getParent();
                    if (graph.removeEdge(st, table) != null) {
                        msg = REMOVE_DEP.formatted(st.getQualifiedName(), table.getQualifiedName());
                        LOG.info(msg);
                    }
                }
            }
        }
    }

    private void processDeps(PgStatement st) {
        for (GenericColumn dep : st.getDeps()) {
            PgStatement depSt = db.getStatement(dep);
            if (depSt != null && !st.equals(depSt)) {
                graph.addEdge(st, depSt);
            }
        }
    }

    /**
     * The only way to find this depcy is to compare refcolumns against all existing unique
     * contraints/keys in reftable.
     * Unfortunately they might not exist at the stage where {@link PgStatement#getDeps()}
     * are populated so we have to defer their lookup until here.
     */
    private void createFkeyToUnique(IConstraintFk con) {
        Collection<String> refs = con.getForeignColumns();
        if (refs.isEmpty()) {
            return;
        }

        IStatement cont = db.getStatement(
                new GenericColumn(con.getForeignSchema(), con.getForeignTable(), DbObjType.TABLE));

        if (cont instanceof PgStatementContainer c) {
            for (AbstractConstraint refCon : c.getConstraints()) {
                if (refCon instanceof IConstraintPk && refs.equals(refCon.getColumns())) {
                    graph.addEdge((PgStatement) con, refCon);
                }
            }
            for (AbstractIndex refInd : c.getIndexes()) {
                if (refInd.isUnique() && refInd.compareColumns(refs)) {
                    graph.addEdge((PgStatement) con, refInd);
                }
            }
        }
    }

    /**
     * Creates the connection between the column of a partitioned table and the
     * columns of its sections (child tables).
     * <br />
     * Partitioned tables cannot use the inheritance mechanism, as in simple tables.
     */
    private void createChildColToPartTblCol(PartitionPgTable tbl, PgColumn col) {
        for (Inherits in : tbl.getInherits()) {
            IStatement parentTbl = db.getStatement(new GenericColumn(in.getKey(), in.getValue(), DbObjType.TABLE));
            if (parentTbl == null) {
                var msg = Messages.DepcyGraph_log_no_such_table.formatted(in.getQualifiedName());
                LOG.error(msg);
                continue;
            }

            if (parentTbl instanceof PartitionPgTable partTable) {
                createChildColToPartTblCol(partTable, col);
            } else {
                String colName = col.getName();
                AbstractColumn parentCol = ((AbstractTable) parentTbl).getColumn(colName);
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
     * @param depcies list of custom dependency pairs to add
     */
    public void addCustomDepcies(List<Entry<PgStatement, PgStatement>> depcies) {
        if (depcies == null) {
            return;
        }
        for (Entry<PgStatement, PgStatement> depcy : depcies) {
            graph.addEdge(depcy.getKey(), depcy.getValue());
        }
    }
}
