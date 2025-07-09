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
 *
 * Copyright 2006 StartNet s.r.o.
 *
 * Distributed under MIT license
 *******************************************************************************/
package org.pgcodekeeper.core.parsers.antlr.expr;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pgcodekeeper.core.FILES_POSTFIX;
import org.pgcodekeeper.core.PgDiffUtils;
import org.pgcodekeeper.core.TestUtils;
import org.pgcodekeeper.core.Utils;
import org.pgcodekeeper.core.loader.FullAnalyze;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.schema.AbstractDatabase;
import org.pgcodekeeper.core.schema.IRelation;
import org.pgcodekeeper.core.schema.meta.MetaContainer;
import org.pgcodekeeper.core.schema.meta.MetaUtils;
import org.pgcodekeeper.core.settings.CoreSettings;
import org.pgcodekeeper.core.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Tests for checking column types.
 *
 * @author shamsutdinov_lr
 */

class ExprTypeTest {

    @ParameterizedTest
    @ValueSource(strings = {
            // Check types in columns of asterisk in view.
            // FIXME "check_types_aster_cols_view",
            // Check types in columns of view.
            // FIXME "check_types_cols_view",
            // Check types in columns of view (extended).
            // FIXME "check_types_cols_view_extended",
            // Check types in table-less columns.
            // FIXME "check_tableless_cols_types",
            // Check array types.
            // FIXME "check_array_types",
            // Check ANY type-matching
            "check_anytype_resolution",
            // Check by named notation type
            "check_named_notation",
    })
    void runCheck(String fileNameTemplate) throws IOException, InterruptedException {
        var settings = new CoreSettings();
        String typesForCompare = TestUtils.readResource(
                fileNameTemplate + FILES_POSTFIX.DIFF_SQL, getClass());

        runDiff(fileNameTemplate, settings, typesForCompare);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            // Compare the types between an asterisk and an ordinary in view.
            "compare_types_aster_ord_view",
    })
    void runCompare(String fileNameTemplate) throws IOException, InterruptedException {
        var settings = new CoreSettings();
        String typesForCompare = getRelationColumnsTypes(
                loadAndAnalyze(fileNameTemplate, settings, FILES_POSTFIX.ORIGINAL_SQL));

        runDiff(fileNameTemplate, settings, typesForCompare);
    }

    private void runDiff(String fileNameTemplate, CoreSettings settings, String typesForCompare)
            throws InterruptedException, IOException {
        MetaContainer dbNew = loadAndAnalyze(fileNameTemplate, settings, FILES_POSTFIX.NEW_SQL);
        Assertions.assertEquals(typesForCompare,
                getRelationColumnsTypes(dbNew), "File: " + fileNameTemplate);
    }

    private MetaContainer loadAndAnalyze(String fileNameTemplate, CoreSettings settings, FILES_POSTFIX postfix)
            throws InterruptedException, IOException {
        AbstractDatabase dbNew = TestUtils.loadTestDump(
                fileNameTemplate + postfix, ExprTypeTest.class, settings, false);
        MetaContainer metaDb = MetaUtils.createTreeFromDb(dbNew);
        FullAnalyze.fullAnalyze(dbNew, metaDb, new ArrayList<>());
        return metaDb;
    }

    private String getRelationColumnsTypes(MetaContainer meta) {
        StringBuilder cols = new StringBuilder();

        for (Entry<String, Map<String, IRelation>> entry : meta.getRelations().entrySet()) {
            String schemaName = entry.getKey();
            if (Utils.isPgSystemSchema(schemaName)) {
                continue;
            }

            boolean isFirstView = true;
            Map<String, IRelation> relations = entry.getValue();
            for (IRelation rel : relations.values()) {
                if (rel.getStatementType() != DbObjType.VIEW) {
                    continue;
                }

                if (isFirstView) {
                    cols.append("\n\nSchema: " + schemaName);
                    isFirstView = false;
                }

                cols.append("\n\n  View: " + rel.getName());
                cols.append("\n    RelationColumns : ");

                for (Pair<String, String> col : PgDiffUtils.sIter(rel.getRelationColumns())) {
                    cols.append("\n     " + col.getFirst() + " - " + col.getSecond());
                }
            }
        }

        return cols.toString();
    }
}