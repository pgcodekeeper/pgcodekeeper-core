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
package org.pgcodekeeper.core.database.base.schema.meta;

import java.util.*;
import java.util.stream.Stream;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.api.schema.ObjectLocation.LocationType;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.database.pg.jdbc.SupportedPgVersion;
import org.pgcodekeeper.core.utils.Pair;

/**
 * Utility class for creating and managing database metadata objects.
 * Provides methods for converting database statements to metadata representations
 * and organizing them into metadata containers.
 */
public final class MetaUtils {

    /**
     * Creates a metadata container from a database object.
     *
     * @param db the database object
     * @return the metadata container with all database objects
     */
    public static MetaContainer createTreeFromDb(AbstractDatabase db) {
        MetaContainer tree = new MetaContainer();
        db.getDescendants()
                .map(MetaUtils::createMetaFromStatement)
                .forEach(tree::addStatement);

        if (db.getDbType() == DatabaseType.PG) {
            MetaStorage.getSystemObjects(db.getVersion()).forEach(tree::addStatement);
        }
        return tree;
    }

    /**
     * Creates a metadata container from a stream of metadata definitions.
     *
     * @param defs    the stream of metadata statements
     * @param dbType  the database type
     * @param version the PostgreSQL version (used only for PG databases)
     * @return the metadata container with all definitions
     */
    public static MetaContainer createTreeFromDefs(Stream<MetaStatement> defs,
                                                   DatabaseType dbType, SupportedPgVersion version) {
        MetaContainer tree = new MetaContainer();
        defs.forEach(tree::addStatement);

        if (dbType == DatabaseType.PG) {
            MetaStorage.getSystemObjects(version).forEach(tree::addStatement);
        }
        return tree;
    }

    private static MetaStatement createMetaFromStatement(AbstractStatement st) {
        DbObjType type = st.getStatementType();
        ObjectLocation loc = getLocation(st, type);
        var meta = createMeta(st, loc);

        String comment = st.getComment();
        if (comment != null) {
            meta.setComment(comment);
        }

        return meta;
    }

    private static MetaStatement createMeta(AbstractStatement st, ObjectLocation loc) {
        if (st instanceof ICast cast) {
            return new MetaCast(cast.getSource(), cast.getTarget(), cast.getContext(), loc);
        }
        if (st instanceof IOperator op) {
            MetaOperator oper = new MetaOperator(loc);
            oper.setLeftArg(op.getLeftArg());
            oper.setRightArg(op.getRightArg());
            oper.setReturns(op.getReturns());
            return oper;
        }
        if (st instanceof IFunction function) {
            MetaFunction func = new MetaFunction(loc, st.getBareName());
            function.getReturnsColumns().forEach(func::addReturnsColumn);
            function.getArguments().forEach(func::addArgument);
            func.setReturns(function.getReturns());
            return func;
        }

        if (st instanceof IConstraintPk c) {
            MetaConstraint con = new MetaConstraint(loc);
            con.setPrimaryKey(c.isPrimaryKey());
            c.getColumns().forEach(con::addColumn);
            return con;
        }

        if (st instanceof IRelation r) {
            MetaRelation rel = new MetaRelation(loc);
            Stream<Pair<String, String>> columns = r.getRelationColumns();
            if (columns != null) {
                rel.addColumns(columns.toList());
            }
            return rel;
        }

        if (st instanceof ICompositeType t) {
            MetaCompositeType composite = new MetaCompositeType(loc);
            t.getAttrs().forEach(e -> composite.addAttr(e.getFirst(), e.getSecond()));
            return composite;
        }
        return new MetaStatement(loc);
    }

    private static ObjectLocation getLocation(AbstractStatement st, DbObjType type) {
        ObjectLocation loc = st.getLocation();
        // some children may have a parental location
        if (loc != null && loc.getType() == type) {
            return loc;
        }
        GenericColumn gc = st.toGenericColumn(type);

        return new ObjectLocation.Builder()
                .setObject(gc)
                .setLocationType(LocationType.DEFINITION)
                .build();
    }

    /**
     * Returns object definitions grouped by file path.
     *
     * @param db the database object
     * @return map of file paths to lists of metadata statements
     */
    public static Map<String, List<MetaStatement>> getObjDefinitions(AbstractDatabase db) {
        Map<String, List<MetaStatement>> definitions = new HashMap<>();

        db.getDescendants().forEach(st -> {
            ObjectLocation loc = st.getLocation();
            if (loc != null) {
                String filePath = loc.getFilePath();
                if (filePath != null) {
                    definitions.computeIfAbsent(filePath, k -> new ArrayList<>())
                            .add(MetaUtils.createMetaFromStatement(st));
                }
            }
        });

        return definitions;
    }

    /**
     * Initializes a view with column information in the metadata container.
     *
     * @param meta       the metadata container
     * @param schemaName the schema name
     * @param name       the view name
     * @param columns    the list of column name-type pairs
     */
    public static void initializeView(MetaContainer meta, String schemaName,
                                      String name, List<? extends Pair<String, String>> columns) {
        IRelation rel = meta.findRelation(schemaName, name);
        if (rel instanceof MetaRelation metaRel) {
            metaRel.addColumns(columns);
        }
    }

    private MetaUtils() {
    }
}
