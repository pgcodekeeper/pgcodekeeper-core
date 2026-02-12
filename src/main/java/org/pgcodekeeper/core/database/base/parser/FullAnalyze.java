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
package org.pgcodekeeper.core.database.base.parser;

import org.pgcodekeeper.core.database.api.launcher.IAnalysisLauncher;
import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.api.schema.IRelation;
import org.pgcodekeeper.core.database.api.schema.ObjectLocation;
import org.pgcodekeeper.core.database.base.schema.meta.MetaContainer;
import org.pgcodekeeper.core.database.base.schema.meta.MetaUtils;
import org.pgcodekeeper.core.database.pg.parser.launcher.PgAggregateAnalysisLauncher;
import org.pgcodekeeper.core.database.pg.parser.launcher.PgOperatorAnalysisLauncher;
import org.pgcodekeeper.core.database.pg.parser.launcher.PgViewAnalysisLauncher;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

/**
 * Performs full analysis of database objects including operators, aggregates, views, and other database elements.
 * Manages ANTLR tasks for concurrent analysis and collects references and dependencies.
 */
public final class FullAnalyze {

    private final List<Object> errors;
    private final List<ObjectLocation> refs = new ArrayList<>();
    private final Queue<AntlrTask<?>> antlrTasks = new ArrayDeque<>();
    private final IDatabase db;
    private final MetaContainer meta;

    private FullAnalyze(IDatabase db, MetaContainer meta, List<Object> errors) {
        this.db = db;
        this.meta = meta;
        this.errors = errors;
    }

    /**
     * Performs full analysis of the database using metadata created from the database.
     *
     * @param db     the database to analyze
     * @param errors list to collect analysis errors
     * @throws InterruptedException if analysis is interrupted
     * @throws IOException          if analysis fails
     */
    public static void fullAnalyze(IDatabase db, List<Object> errors)
            throws InterruptedException, IOException {
        fullAnalyze(db, MetaUtils.createTreeFromDb(db), errors);
    }

    /**
     * Performs full analysis of the database using the provided metadata container.
     *
     * @param db     the database to analyze
     * @param metaDb metadata container for analysis context
     * @param errors list to collect analysis errors
     * @throws InterruptedException if analysis is interrupted
     * @throws IOException          if analysis fails
     */
    public static void fullAnalyze(IDatabase db, MetaContainer metaDb, List<Object> errors)
            throws InterruptedException, IOException {
        new FullAnalyze(db, metaDb, errors).fullAnalyze();
    }

    private void fullAnalyze() throws InterruptedException, IOException {
        analyzeOperators();
        analyzeAggregate();
        analyzeView(null);

        for (IAnalysisLauncher l : db.getAnalysisLaunchers()) {
            if (l != null) {
                AntlrTaskManager.submit(antlrTasks,
                        () -> l.launchAnalyze(errors, meta),
                        dependencies -> {
                            var st = l.getStmt();
                            dependencies.forEach(st::addDependency);
                            refs.addAll(l.getReferences());
                        });
            }
        }
        db.clearAnalysisLaunchers();
        AntlrTaskManager.finish(antlrTasks);

        for (ObjectLocation ref : refs) {
            db.addReference(ref.getFilePath(), ref);
        }
    }

    /**
     * Analyzes views in the database, optionally focusing on a specific relation.
     *
     * @param rel the specific relation to analyze, or null to analyze all views
     */
    public void analyzeView(IRelation rel) {
        List<IAnalysisLauncher> launchers = db.getAnalysisLaunchers();
        for (int i = 0; i < launchers.size(); ++i) {
            var l = launchers.get(i);
            if (l instanceof PgViewAnalysisLauncher v
                    && (rel == null
                    || (rel.getSchemaName().equals(l.getSchemaName())
                    && rel.getName().equals(l.getStmt().getName())))) {
                // allow GC to reclaim context memory immediately
                // and protects from infinite recursion
                launchers.set(i, null);
                v.setFullAnalyze(this);
                var st = l.getStmt();
                var dependencies = l.launchAnalyze(errors, meta);
                dependencies.forEach(st::addDependency);
                refs.addAll(l.getReferences());
            }
        }
    }

    private void analyzeOperators() {
        List<IAnalysisLauncher> launchers = db.getAnalysisLaunchers();
        for (int i = 0; i < launchers.size(); ++i) {
            var l = launchers.get(i);
            if (l instanceof PgOperatorAnalysisLauncher) {
                // allow GC to reclaim context memory immediately
                launchers.set(i, null);
                l.launchAnalyze(errors, meta);
            }
        }
    }

    private void analyzeAggregate() {
        List<IAnalysisLauncher> launchers = db.getAnalysisLaunchers();
        for (int i = 0; i < launchers.size(); ++i) {
            var l = launchers.get(i);
            if (l instanceof PgAggregateAnalysisLauncher) {
                // allow GC to reclaim context memory immediately
                launchers.set(i, null);
                l.launchAnalyze(errors, meta);
            }
        }
    }
}
