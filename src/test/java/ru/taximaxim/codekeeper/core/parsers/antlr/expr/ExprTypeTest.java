/**
 * Copyright 2006 StartNet s.r.o.
 *
 * Distributed under MIT license
 */
package ru.taximaxim.codekeeper.core.parsers.antlr.expr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import ru.taximaxim.codekeeper.core.FILES_POSTFIX;
import ru.taximaxim.codekeeper.core.PgDiffArguments;
import ru.taximaxim.codekeeper.core.PgDiffUtils;
import ru.taximaxim.codekeeper.core.TestUtils;
import ru.taximaxim.codekeeper.core.Utils;
import ru.taximaxim.codekeeper.core.loader.FullAnalyze;
import ru.taximaxim.codekeeper.core.model.difftree.DbObjType;
import ru.taximaxim.codekeeper.core.schema.IRelation;
import ru.taximaxim.codekeeper.core.schema.PgDatabase;
import ru.taximaxim.codekeeper.core.schema.meta.MetaContainer;
import ru.taximaxim.codekeeper.core.schema.meta.MetaUtils;
import ru.taximaxim.codekeeper.core.utils.Pair;

/**
 * Tests for checking column types.
 *
 * @author shamsutdinov_lr
 */

public class ExprTypeTest {

    private static final String CHECK = "check_";
    private static final String COMPARE = "compare_";

    @ParameterizedTest
    @ValueSource(strings = {
            // Compare the types between an asterisk and an ordinary in view.
            COMPARE + "types_aster_ord_view",
            // Check types in columns of asterisk in view.
            CHECK + "types_aster_cols_view",
            // Check types in columns of view.
            CHECK + "types_cols_view",
            // Check types in columns of view (extended).
            CHECK + "types_cols_view_extended",
            // Check types in table-less columns.
            CHECK + "tableless_cols_types",
            // Check array types.
            CHECK + "array_types",
            // Check ANY type-matching
            CHECK + "anytype_resolution",
            // Check by named notation type
            CHECK + "named_notation",
    })

    public void runDiff(String fileNameTemplate) throws IOException, InterruptedException {
        PgDiffArguments args = new PgDiffArguments();
        MetaContainer dbNew = loadAndAnalyze(fileNameTemplate, args, FILES_POSTFIX.NEW_SQL);

        String typesForCompare = null;
        if (fileNameTemplate.startsWith(CHECK)) {
            // compare with a "type-dump"
            typesForCompare = TestUtils.inputStreamToString(ExprTypeTest.class
                    .getResourceAsStream(fileNameTemplate + FILES_POSTFIX.DIFF_SQL));
        } else if (fileNameTemplate.startsWith(COMPARE)) {
            // compare with types with another SQL representation
            typesForCompare = getRelationColumnsTypes(
                    loadAndAnalyze(fileNameTemplate, args, FILES_POSTFIX.ORIGINAL_SQL));
        }

        Assertions.assertEquals(typesForCompare,
                getRelationColumnsTypes(dbNew), "File: " + fileNameTemplate);
    }

    private MetaContainer loadAndAnalyze(String fileNameTemplate, PgDiffArguments args, FILES_POSTFIX postfix)
            throws InterruptedException, IOException {
        PgDatabase dbNew = TestUtils.loadTestDump(
                fileNameTemplate + postfix, ExprTypeTest.class, args, false);
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