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

import org.jgrapht.event.TraversalListenerAdapter;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.DepthFirstIterator;
import org.pgcodekeeper.core.schema.PgStatement;

import java.util.Collections;
import java.util.List;

/**
 * Utility class for working with dependency graphs.
 * Provides methods for traversing dependency graphs in forward and reverse directions
 * using depth-first search algorithms.
 */
public final class GraphUtils {

    /**
     * Gets all statements that depend on the given statement (reverse dependencies).
     * Performs depth-first search traversal to find ALL dependent objects at any depth level,
     * without limitations on traversal depth. This ensures complete dependency resolution
     * for complex multi-level dependency chains.
     * 
     * @param depcyGraph the dependency graph to traverse
     * @param statement the statement to find dependents for
     * @return list of all statements that directly or indirectly depend on the given statement
     */
    public static List<PgStatement> reverse(DepcyGraph depcyGraph, PgStatement statement) {
        if (!depcyGraph.getReversedGraph().containsVertex(statement)) {
            return Collections.emptyList();
        }
        var adapter = new CollectingTraversalAdapter(statement);
        var dfi = new DepthFirstIterator<>(depcyGraph.getReversedGraph(), statement);
        iterate(dfi, adapter);
        return adapter.getStatements();
    }

    /**
     * Gets all statements that the given statement depends on (forward dependencies).
     * Performs depth-first search traversal to find ALL dependency objects at any depth level,
     * without limitations on traversal depth. This ensures complete dependency resolution
     * for complex multi-level dependency chains.
     * 
     * @param depcyGraph the dependency graph to traverse
     * @param statement the statement to find dependencies for
     * @return list of all statements that the given statement directly or indirectly depends on
     */
    public static List<PgStatement> forward(DepcyGraph depcyGraph, PgStatement statement) {
        if (!depcyGraph.getGraph().containsVertex(statement)) {
            return Collections.emptyList();
        }
        var adapter = new CollectingTraversalAdapter(statement);
        var dfi = new DepthFirstIterator<>(depcyGraph.getGraph(), statement);
        iterate(dfi, adapter);
        return adapter.getStatements();
    }

    /**
     * Iterates through the graph using depth-first iterator and collects objects.
     *
     * @param dfi the depth-first iterator for traversal
     * @param adapter the adapter for collecting traversed objects
     */
    private static void iterate(DepthFirstIterator<PgStatement, DefaultEdge> dfi,
                                TraversalListenerAdapter<PgStatement, DefaultEdge> adapter) {
        dfi.addTraversalListener(adapter);
        while (dfi.hasNext()) {
            dfi.next();
        }
    }

    private GraphUtils() {
        // only statics
    }
}
