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
import org.jgrapht.graph.DefaultEdge;
import org.pgcodekeeper.core.schema.AbstractDatabase;
import org.pgcodekeeper.core.schema.PgStatement;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Simple dependency resolver for database objects.
 * Provides methods to find create and drop dependencies for database statements
 * using dependency graphs built from old and new database schemas.
 */
public class SimpleDepcyResolver {

    private final AbstractDatabase oldDb;
    private final AbstractDatabase newDb;
    private final DepcyGraph oldDepcyGraph;
    private final DepcyGraph newDepcyGraph;

    /**
     * Creates a dependency resolver with old database only.
     *
     * @param oldDatabase   the old database schema
     * @param isShowColumns whether to show column dependencies
     */
    public SimpleDepcyResolver(AbstractDatabase oldDatabase, boolean isShowColumns) {
        this(oldDatabase, null, isShowColumns);
    }

    /**
     * Creates a dependency resolver with both old and new database schemas.
     *
     * @param oldDatabase   the old database schema
     * @param newDatabase   the new database schema, can be null
     * @param isShowColumns whether to show column dependencies
     */
    public SimpleDepcyResolver(AbstractDatabase oldDatabase, AbstractDatabase newDatabase, boolean isShowColumns) {
        this.oldDb = oldDatabase;
        this.newDb = newDatabase;
        this.oldDepcyGraph = new DepcyGraph(oldDatabase, !isShowColumns);
        this.newDepcyGraph = newDatabase == null ? null : new DepcyGraph(newDatabase, !isShowColumns);
    }

    /**
     * Gets dependencies required for creating a statement.
     * Returns forward dependencies from the new database schema.
     *
     * @param toCreate the statement to create
     * @return collection of statements that must be created before the target statement
     * @throws IllegalStateException if new database is not defined
     */
    public Collection<PgStatement> getCreateDepcies(PgStatement toCreate) {
        if (newDb == null) {
            throw new IllegalStateException("New database not defined");
        }

        PgStatement statement = toCreate.getTwin(newDb);
        var dependencies = GraphUtils.forward(newDepcyGraph, statement);
        dependencies.add(statement);
        return dependencies;
    }

    /**
     * Gets dependencies that must be dropped when dropping a statement.
     * Returns reverse dependencies from the old database schema.
     *
     * @param toDrop the statement to drop
     * @return collection of statements that depend on the target statement and must be dropped first
     */
    public Collection<PgStatement> getDropDepcies(PgStatement toDrop) {
        PgStatement statement = toDrop.getTwin(oldDb);
        var dependents = GraphUtils.reverse(oldDepcyGraph, statement);
        dependents.add(statement);
        return dependents;
    }

    /**
     * Gets statements that the given entity is directly connected to.
     * Returns outgoing edges from the entity in the old database dependency graph.
     *
     * @param entity the statement to find connections for
     * @return set of statements that the entity depends on
     */
    public Set<PgStatement> getConnectedTo(PgStatement entity) {
        Set<PgStatement> connected = new HashSet<>();
        Graph<PgStatement, DefaultEdge> currentGraph = oldDepcyGraph.getGraph();
        for (DefaultEdge e : currentGraph.outgoingEdgesOf(entity)) {
            connected.add(currentGraph.getEdgeTarget(e));
        }

        return connected;
    }
}
