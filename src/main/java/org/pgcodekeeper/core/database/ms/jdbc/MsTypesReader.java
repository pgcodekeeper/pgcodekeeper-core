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
package org.pgcodekeeper.core.database.ms.jdbc;

import org.pgcodekeeper.core.database.api.schema.ISimpleColumnContainer;
import org.pgcodekeeper.core.database.api.schema.IStatementContainer;
import org.pgcodekeeper.core.database.base.jdbc.AbstractSearchPathJdbcReader;
import org.pgcodekeeper.core.database.base.schema.AbstractColumn;
import org.pgcodekeeper.core.database.base.schema.AbstractSchema;
import org.pgcodekeeper.core.database.api.schema.GenericColumn;
import org.pgcodekeeper.core.database.base.schema.SimpleColumn;
import org.pgcodekeeper.core.database.ms.loader.MsJdbcLoader;
import org.pgcodekeeper.core.database.base.jdbc.QueryBuilder;
import org.pgcodekeeper.core.exception.XmlReaderException;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.ms.schema.MsConstraintCheck;
import org.pgcodekeeper.core.database.ms.schema.MsConstraintPk;
import org.pgcodekeeper.core.database.ms.schema.MsIndex;
import org.pgcodekeeper.core.database.ms.schema.MsType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Reader for Microsoft SQL types.
 * Loads type definitions from sys.types system view.
 */
public final class MsTypesReader extends AbstractSearchPathJdbcReader<MsJdbcLoader> implements IMsJdbcReader {

    /**
     * Constructs a new Microsoft SQL types reader.
     *
     * @param loader the JDBC loader base for database operations
     */
    public MsTypesReader(MsJdbcLoader loader) {
        super(loader);
    }

    @Override
    protected void processResult(ResultSet res, AbstractSchema schema)
            throws SQLException, XmlReaderException {
        String name = res.getString("name");
        loader.setCurrentObject(new GenericColumn(schema.getName(), name, DbObjType.TYPE));

        MsType type = new MsType(name);
        String baseType = res.getString("base_type");
        String assembly = res.getString("assembly_class");
        if (assembly != null) {
            type.setAssemblyName(assembly);
            type.setAssemblyClass(res.getString("assembly_class"));
            type.addDependency(new GenericColumn(assembly, DbObjType.ASSEMBLY));
        } else if (baseType != null) {
            type.setBaseType(MsJdbcLoader.getMsType(type, null, baseType, false, res.getInt("size"),
                    res.getInt("precision"), res.getInt("scale")));
            type.setNotNull(!res.getBoolean("is_nullable"));
        } else {
            addColumns(res, schema, type);
            addIndices(type, MsXmlReader.readXML(res.getString("indices")));
            addChecks(type, MsXmlReader.readXML(res.getString("checks")));
            type.setMemoryOptimized(res.getBoolean("is_memory_optimized"));
        }

        schema.addType(type);
        loader.setOwner(type, res.getString("owner"));
        loader.setPrivileges(type, MsXmlReader.readXML(res.getString("acl")));
    }

    private void addColumns(ResultSet res, AbstractSchema schema, MsType type)
            throws XmlReaderException, SQLException {
        for (MsXmlReader col : MsXmlReader.readXML(res.getString("cols"))) {
            // pass the 'type' to the method for extract type depcy from column
            // object since it is temporary
            AbstractColumn column = MsTablesReader.getColumn(col, schema, loader, type);
            type.addChild(column);
            // extract type depcy from column object since it is temporary
            // (column also has depcy that is not related to the analysis)
            column.getDependencies().forEach(type::addDependency);
        }
    }

    private void addIndices(IStatementContainer type, List<MsXmlReader> indices) throws XmlReaderException {
        for (MsXmlReader index : indices) {
            boolean isPrimaryKey = index.getBoolean("pk");
            boolean isUniqueConstraint = index.getBoolean("uc");
            boolean isClustered = index.getBoolean("cl");
            int bucketCount = index.getInt("bc");
            String filter = index.getString("def");
            var cols = MsXmlReader.readXML(index.getString("cols"));

            if (isPrimaryKey || isUniqueConstraint) {
                var constrPk = new MsConstraintPk(null, isPrimaryKey);
                constrPk.setClustered(isClustered);
                fillColumns(constrPk, cols);
                if (index.getBoolean("dk")) {
                    constrPk.addOption("IGNORE_DUP_KEY", "ON");
                } else if (bucketCount > 0) {
                    constrPk.addOption("BUCKET_COUNT", Integer.toString(bucketCount));
                }
                type.addChild(constrPk);
            } else {
                var idx = new MsIndex(index.getString("name"));
                idx.setClustered(isClustered);
                fillColumns(idx, cols);
                if (filter != null) {
                    // broken in dump
                    idx.setWhere(filter);
                }
                if (bucketCount > 0) {
                    idx.addOption("BUCKET_COUNT", Integer.toString(bucketCount));
                }
                type.addChild(idx);
            }
        }
    }

    private void fillColumns(ISimpleColumnContainer stmt, List<MsXmlReader> cols) {
        for (MsXmlReader col : cols) {
            String colName = col.getString("name");
            var simpCol = new SimpleColumn(colName);
            simpCol.setDesc(col.getBoolean("is_desc"));
            stmt.addColumn(simpCol);
        }
    }

    private void addChecks(IStatementContainer type, List<MsXmlReader> checks) {
        for (MsXmlReader check : checks) {
            var constrCh = new MsConstraintCheck(null);
            constrCh.setExpression(check.getString("def"));
            type.addChild(constrCh);
        }
    }

    @Override
    protected void fillQueryBuilder(QueryBuilder builder) {
        addMsPrivilegesPart(builder);
        addMsOwnerPart(builder);

        builder
                .column("res.name")
                .column("a.name AS assembly")
                .column("ay.assembly_class")
                .column("res.is_nullable")
                .column("basetypes.name AS base_type")
                .column("CASE WHEN basetypes.name IN (N'nchar', N'nvarchar') AND res.max_length >= 0 THEN res.max_length/2 ELSE res.max_length END AS size")
                .column("res.precision")
                .column("res.scale")
                .column("ttt.is_memory_optimized")
                .from("sys.types res WITH (NOLOCK)")
                .join("LEFT JOIN sys.types basetypes WITH (NOLOCK) ON res.system_type_id=basetypes.system_type_id AND basetypes.system_type_id=basetypes.user_type_id")
                .join("LEFT JOIN sys.assembly_types ay WITH (NOLOCK) ON ay.user_type_id=res.user_type_id")
                .join("LEFT JOIN sys.assemblies a WITH (NOLOCK) ON a.assembly_id=ay.assembly_id")
                .join("LEFT JOIN sys.table_types ttt WITH (NOLOCK) ON ttt.user_type_id=res.user_type_id")
                .where("res.is_user_defined=1");

        // after join ttt
        addMsConstraintsPart(builder);
        addMsColumnsPart(builder);
        addMsIndicesPart(builder);
    }

    private void addMsConstraintsPart(QueryBuilder builder) {
        QueryBuilder checks = new QueryBuilder()
                .column("c.definition AS def")
                .from("sys.check_constraints c WITH (NOLOCK)")
                .where("c.parent_object_id=ttt.type_table_object_id")
                .postAction("FOR XML RAW, ROOT");

        builder.column("ch.checks");
        builder.join("CROSS APPLY", checks, "ch (checks)");
    }

    private void addMsColumnsPart(QueryBuilder builder) {
        QueryBuilder subSelect = new QueryBuilder()
                .column("c.name")
                .column("ss.name AS st")
                .column("usrt.name AS type")
                .column("usrt.is_user_defined AS ud")
                .column("CASE WHEN c.max_length >= 0 AND baset.name IN (N'nchar', N'nvarchar') THEN c.max_length/2 ELSE c.max_length END AS size")
                .column("c.precision AS pr")
                .column("c.scale AS sc")
                .column("c.collation_name AS cn")
                .column("object_definition(c.default_object_id) AS dv")
                .column("c.is_nullable AS nl")
                .column("c.is_identity AS ii")
                .column("ic.seed_value AS s")
                .column("ic.increment_value AS i")
                .column("ic.is_not_for_replication AS nfr")
                .column("c.is_rowguidcol AS rgc")
                .column("cc.is_persisted AS ps")
                .column("cc.definition AS def")
                .from("sys.all_columns c WITH (NOLOCK)")
                .join("LEFT OUTER JOIN sys.computed_columns cc WITH (NOLOCK) ON cc.object_id = c.object_id AND cc.column_id = c.column_id")
                .join("LEFT OUTER JOIN sys.identity_columns ic WITH (NOLOCK) ON ic.object_id = c.object_id AND ic.column_id = c.column_id")
                .join("LEFT OUTER JOIN sys.types usrt WITH (NOLOCK) ON usrt.user_type_id = c.user_type_id")
                .join("LEFT OUTER JOIN sys.schemas ss WITH (NOLOCK) ON ss.schema_id = usrt.schema_id")
                .join("LEFT OUTER JOIN sys.types baset WITH (NOLOCK) ON (baset.user_type_id = c.system_type_id AND baset.user_type_id = baset.system_type_id)")
                .join("  OR (baset.system_type_id = c.system_type_id AND baset.user_type_id = c.user_type_id AND baset.is_user_defined = 0 AND baset.is_assembly_type = 1)")
                .where("ttt.type_table_object_id=c.object_id")
                .orderBy("c.column_id")
                .postAction("FOR XML RAW, ROOT");

        builder.column("cc.cols");
        builder.join("CROSS APPLY", subSelect, "cc (cols)");
    }

    private void addMsIndicesPart(QueryBuilder builder) {
        QueryBuilder subSubSelect = new QueryBuilder()
                .column("sc.name")
                .column("c.is_descending_key AS is_desc")
                .from("sys.index_columns c WITH (NOLOCK)")
                .join("JOIN sys.columns sc WITH (NOLOCK) ON c.object_id = sc.object_id AND c.column_id = sc.column_id")
                .where("c.object_id = si.object_id")
                .where("c.index_id = si.index_id")
                .orderBy("c.index_column_id")
                .postAction("FOR XML RAW, ROOT");

        QueryBuilder subSelect = new QueryBuilder()
                .column("si.name")
                .column("si.is_primary_key AS pk")
                .column("si.is_unique_constraint AS uc")
                .column("si.ignore_dup_key AS dk")
                .column("INDEXPROPERTY(si.object_id, si.name, 'IsClustered') AS cl")
                .column("hi.bucket_count AS bc")
                .column("ccc.cols")
                .column("si.filter_definition AS def")
                .from("sys.indexes AS si WITH (NOLOCK)")
                .join("LEFT JOIN sys.objects o WITH (NOLOCK) ON si.object_id = o.object_id")
                .join("LEFT JOIN sys.hash_indexes hi WITH (NOLOCK) ON hi.object_id = si.object_id AND hi.index_id = si.index_id")
                .join("CROSS APPLY", subSubSelect, "ccc (cols)")
                .where("si.object_id=ttt.type_table_object_id")
                .where("si.index_id > 0")
                .where("si.is_hypothetical = 0")
                .postAction("FOR XML RAW, ROOT");

        builder.column("ii.indices");
        builder.join("CROSS APPLY", subSelect, "ii (indices)");
    }

    @Override
    protected String getSchemaColumn() {
        return "res.schema_id";
    }

    @Override
    public QueryBuilder formatMsPrivileges(QueryBuilder privileges) {
        return privileges
                .where("major_id = res.user_type_id")
                .where("perm.class = 6");
    }
}
