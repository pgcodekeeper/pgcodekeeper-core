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
package org.pgcodekeeper.core.database.base.schema.meta;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.schema.AbstractDatabase;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.loader.pg.SupportedPgVersion;
import org.pgcodekeeper.core.database.api.schema.ObjectLocation.LocationType;
import org.pgcodekeeper.core.database.pg.schema.PgCompositeType;
import org.pgcodekeeper.core.utils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

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
        MetaStatement meta = switch (type) {
            case CAST -> {
                ICast cast = (ICast) st;
                yield new MetaCast(cast.getSource(), cast.getTarget(), cast.getContext(), loc);
            }
            case OPERATOR -> {
                IOperator operator = (IOperator) st;
                MetaOperator oper = new MetaOperator(loc);
                oper.setLeftArg(operator.getLeftArg());
                oper.setRightArg(operator.getRightArg());
                oper.setReturns(operator.getReturns());
                yield oper;
            }
            case AGGREGATE, FUNCTION, PROCEDURE -> {
                IFunction funcion = (IFunction) st;
                MetaFunction func = new MetaFunction(loc, st.getBareName());
                funcion.getReturnsColumns().forEach(func::addReturnsColumn);
                funcion.getArguments().forEach(func::addArgument);
                func.setReturns(funcion.getReturns());
                yield func;
            }
            case CONSTRAINT -> {
                MetaConstraint con = new MetaConstraint(loc);
                con.setPrimaryKey(((IConstraint) st).isPrimaryKey());
                ((IConstraint) st).getColumns().forEach(con::addColumn);
                yield con;
            }
            case SEQUENCE, TABLE, DICTIONARY, VIEW -> {
                MetaRelation rel = new MetaRelation(loc);
                Stream<Pair<String, String>> columns = ((IRelation) st).getRelationColumns();
                if (columns != null) {
                    rel.addColumns(columns.toList());
                }
                yield rel;
            }
            case TYPE -> {
                if (st instanceof PgCompositeType compositeType) {
                    MetaCompositeType composite = new MetaCompositeType(loc);
                    compositeType.getAttrs().forEach(e -> composite.addAttr(e.getName(), e.getType()));
                    yield composite;
                }
                yield new MetaStatement(loc);
            }
            default -> new MetaStatement(loc);
        };

        String commnent = st.getComment();
        if (commnent != null) {
            meta.setComment(commnent);
        }

        return meta;
    }

    private static ObjectLocation getLocation(AbstractStatement st, DbObjType type) {
        ObjectLocation loc = st.getLocation();
        // some children may have a parental location
        if (loc != null && loc.getType() == type) {
            return loc;
        }
        GenericColumn gc;
        switch (type) {
            case CAST:
            case USER_MAPPING:
            case SCHEMA:
            case EXTENSION:
            case EVENT_TRIGGER:
            case FOREIGN_DATA_WRAPPER:
            case SERVER:
            case ROLE:
            case USER:
            case ASSEMBLY:
                gc = new GenericColumn(st.getName(), type);
                break;
            case COLLATION:
            case AGGREGATE:
            case DOMAIN:
            case FTS_CONFIGURATION:
            case FTS_DICTIONARY:
            case FTS_PARSER:
            case FTS_TEMPLATE:
            case OPERATOR:
            case PROCEDURE:
            case SEQUENCE:
            case TABLE:
            case DICTIONARY:
            case TYPE:
            case VIEW:
            case STATISTICS:
                gc = new GenericColumn(st.getParent().getName(), st.getName(), type);
                break;
            case INDEX:
                gc = new GenericColumn(st.getParent().getParent().getName(), st.getName(), type);
                break;
            case CONSTRAINT:
            case RULE:
            case TRIGGER:
                IStatement parent = st.getParent();
                gc = new GenericColumn(parent.getParent().getName(), parent.getName(), st.getName(), type);
                break;
            case POLICY:
                if (st.getDbType() == DatabaseType.CH) {
                    gc = new GenericColumn(st.getName(), type);
                } else {
                    parent = st.getParent();
                    gc = new GenericColumn(parent.getParent().getName(), parent.getName(), st.getName(), type);
                }
                break;
            case FUNCTION:
                if (st.getDbType() == DatabaseType.CH) {
                    gc = new GenericColumn(st.getName(), type);
                } else {
                    gc = new GenericColumn(st.getParent().getName(), st.getName(), type);
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported type " + type);
        }

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
