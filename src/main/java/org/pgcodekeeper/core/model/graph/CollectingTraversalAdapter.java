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

import org.jgrapht.event.TraversalListenerAdapter;
import org.jgrapht.event.VertexTraversalEvent;
import org.jgrapht.graph.DefaultEdge;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.api.schema.IStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Traversal adapter that collects statements during graph traversal.
 */
final class CollectingTraversalAdapter extends TraversalListenerAdapter<IStatement, DefaultEdge> {

    private final List<IStatement> statements = new ArrayList<>();

    private final IStatement starter;

    /**
     * Creates a new collecting traversal adapter.
     *
     * @param starter the starting statement to exclude from collection
     */
    public CollectingTraversalAdapter(IStatement starter) {
        this.starter = starter;
    }

    @Override
    public void vertexFinished(VertexTraversalEvent<IStatement> e) {
        IStatement statement = e.getVertex();
        if (statement.getStatementType() != DbObjType.DATABASE && statement != starter) {
            statements.add(statement);
        }
    }

    List<IStatement> getStatements() {
        return statements;
    }
}
