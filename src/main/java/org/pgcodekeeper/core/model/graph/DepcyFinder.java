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
import org.jgrapht.graph.DefaultEdge;
import org.pgcodekeeper.core.utils.Utils;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.api.schema.IStatement;
import org.pgcodekeeper.core.database.base.schema.AbstractDatabase;
import org.pgcodekeeper.core.database.base.schema.AbstractTable;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.database.base.schema.StatementUtils;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Database dependency finder for analyzing object dependencies.
 * Provides methods to find dependencies by patterns or specific statements
 * with support for filtering by object types and depth control.
 */
public class DepcyFinder {

    private static final int START_LEVEL = 0;

    private final IDatabase db;
    private final Graph<IStatement, DefaultEdge> graph;
    private final int depth;
    private final EnumSet<DbObjType> filterObjTypes;
    private final boolean isInvertFilter;
    private final List<PrintObj> printObjects = new ArrayList<>();

    private DepcyFinder(IDatabase db, int depth, boolean isReverse,
                        Collection<DbObjType> filterObjTypes, boolean isInvertFilter) {
        this.db = db;
        DepcyGraph dg = new DepcyGraph(db);
        this.graph = isReverse ? dg.getGraph() : dg.getReversedGraph();
        this.depth = depth;
        if (filterObjTypes.isEmpty()) {
            this.filterObjTypes = EnumSet.noneOf(DbObjType.class);
        } else {
            this.filterObjTypes = EnumSet.copyOf(filterObjTypes);
        }
        this.isInvertFilter = isInvertFilter;
    }

    /**
     * Finds dependencies by matching object name patterns.
     * 
     * @param depth maximum depth to search
     * @param isReverse whether to search reverse dependencies
     * @param filterObjTypes object types to filter by
     * @param isInvertFilter whether to invert the filter
     * @param db the database to search in
     * @param names collection of name patterns to match
     * @return list of formatted dependency strings
     */
    public static List<String> byPatterns(int depth, boolean isReverse, Collection<DbObjType> filterObjTypes,
                                          boolean isInvertFilter, AbstractDatabase db, Collection<String> names) {
        DepcyFinder depcyFinder = new DepcyFinder(db, depth, isReverse, filterObjTypes, isInvertFilter);
        depcyFinder.searchDeps(names);
        return depcyFinder.getResult();
    }

    /**
     * Finds dependencies for a specific database statement.
     * 
     * @param depth maximum depth to search
     * @param isReverse whether to search reverse dependencies
     * @param filterObjTypes object types to filter by
     * @param st the statement to find dependencies for
     * @return list of formatted dependency strings
     */
    public static List<String> byStatement(int depth, boolean isReverse, Collection<DbObjType> filterObjTypes,
                                           AbstractStatement st) {
        DepcyFinder depcyFinder = new DepcyFinder(st.getDatabase(), depth, isReverse, filterObjTypes, false);
        depcyFinder.fillTree(st, START_LEVEL, new HashSet<>(), null, 0);
        return depcyFinder.getResult();
    }

    private void searchDeps(Collection<String> names) {
        if (!names.isEmpty()) {
            Map<Pattern, Boolean> patterns = new HashMap<>();
            var escapeList = List.of("\\[", "\"", "\\(");

            for (String name : names) {
                Pattern pattern = Pattern.compile(name, Pattern.CASE_INSENSITIVE);
                boolean isNeedQuotes = Utils.stringContainsAnyItem(name, escapeList);
                patterns.put(pattern, isNeedQuotes);
            }

            db.getDescendants().flatMap(AbstractTable::columnAdder)
                    .filter(st -> find(patterns, st))
                    .forEach(st -> fillTree(st, START_LEVEL, new HashSet<>(), null, 0));
        } else {
            fillTree(db, START_LEVEL, new HashSet<>(), null, 0);
        }
    }

    private boolean find(Map<Pattern, Boolean> patterns, IStatement st) {
        for (var entry : patterns.entrySet()) {
            boolean isNeedQuotes = entry.getValue();
            String objName;
            if (isNeedQuotes) {
                objName = st.getQualifiedName();
            } else {
                objName = StatementUtils.getFullBareName(st);
            }

            Pattern pattern = entry.getKey();
            if (pattern.matcher(objName).matches()) {
                return true;
            }
        }
        return false;
    }

    private void fillTree(IStatement st, int level, Set<IStatement> added, IStatement parentSt, int hiddenObj) {
        DbObjType type = st.getStatementType();

        if (DbObjType.DATABASE == type && START_LEVEL != level) {
            // do not show database in reverse graph
            return;
        }

        boolean isCyclic = !added.add(st);

        final IStatement finalParentSt;
        if (isPrintObj(st)) {
            printObjects.add(new PrintObj(st, parentSt, level, hiddenObj, isCyclic));
            finalParentSt = st;
            hiddenObj = 0;
            level++;
        } else {
            hiddenObj++;
            finalParentSt = parentSt;
        }
        if (depth > level && !isCyclic) {
            final int finalLevel = level;
            final int finalHiddenObj = hiddenObj;

            graph.outgoingEdgesOf(st).stream().map(graph::getEdgeTarget)
                    .sorted(Comparator.comparing(IStatement::getStatementType))
                    .forEach(pgSt -> fillTree(pgSt, finalLevel, new HashSet<>(added), finalParentSt, finalHiddenObj));
        }
    }

    private boolean isPrintObj(IStatement st) {
        DbObjType type = st.getStatementType();

        if (filterObjTypes.isEmpty()) {
            return true;
        }
        if (!isInvertFilter) {
            return filterObjTypes.contains(type);
        }
        return !filterObjTypes.contains(type);
    }

    private List<String> getResult() {
        List<String> result = new ArrayList<>();
        for (PrintObj prObj : printObjects) {
            result.add(prObj.toString());
        }
        return result;
    }
}
