package ru.taximaxim.codekeeper.core.loader.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import ru.taximaxim.codekeeper.core.MsDiffUtils;
import ru.taximaxim.codekeeper.core.loader.JdbcQueries;
import ru.taximaxim.codekeeper.core.model.difftree.DbObjType;
import ru.taximaxim.codekeeper.core.schema.AbstractColumn;
import ru.taximaxim.codekeeper.core.schema.AbstractSchema;
import ru.taximaxim.codekeeper.core.schema.GenericColumn;
import ru.taximaxim.codekeeper.core.schema.MsType;

public class MsTypesReader extends JdbcReader {

    public MsTypesReader(JdbcLoaderBase loader) {
        super(JdbcQueries.QUERY_MS_TYPES, loader);
    }

    @Override
    protected void processResult(ResultSet res, AbstractSchema schema)
            throws SQLException, XmlReaderException {
        loader.monitor.worked(1);
        String name = res.getString("name");
        loader.setCurrentObject(new GenericColumn(schema.getName(), name, DbObjType.TYPE));

        MsType type = new MsType(name);
        String baseType = res.getString("base_type");
        String assembly = res.getString("assembly_class");
        if (assembly != null) {
            type.setAssemblyName(assembly);
            type.setAssemblyClass(res.getString("assembly_class"));
            type.addDep(new GenericColumn(assembly, DbObjType.ASSEMBLY));
        } else if (baseType != null) {
            type.setBaseType(JdbcLoaderBase.getMsType(type, null, baseType, false, res.getInt("size"),
                    res.getInt("precision"), res.getInt("scale")));
            type.setNotNull(!res.getBoolean("is_nullable"));
        } else {
            addColumns(res, schema, type);
            addIndices(type, XmlReader.readXML(res.getString("indices")));
            addChecks(type, XmlReader.readXML(res.getString("checks")));

            type.setMemoryOptimized(res.getBoolean("is_memory_optimized"));
        }

        schema.addType(type);
        loader.setOwner(type, res.getString("owner"));
        loader.setPrivileges(type, XmlReader.readXML(res.getString("acl")));
    }

    private void addColumns(ResultSet res, AbstractSchema schema, MsType type)
            throws XmlReaderException, SQLException {
        for (XmlReader col : XmlReader.readXML(res.getString("cols"))) {
            // pass the 'type' to the method for extract type depcy from column
            // object since it is temporary
            AbstractColumn column = MsTablesReader.getColumn(col, schema, loader, type);
            type.addColumn(column.getFullDefinition());
            // extract type depcy from column object since it is temporary
            // (column also has depcy that is not related to the analysis)
            type.addAllDeps(column.getDeps());
        }
    }

    private void addIndices(MsType type, List<XmlReader> indices) throws XmlReaderException {
        for (XmlReader index : indices) {
            boolean isPrimaryKey = index.getBoolean("pk");
            boolean isUniqueConstraint = index.getBoolean("uc");
            boolean isClustered = index.getBoolean("cl");
            boolean isIgnoreDupKey = index.getBoolean("dk");
            int bucketCount = index.getInt("bc");
            String filter = index.getString("def");

            StringBuilder definition = new StringBuilder();

            if (isPrimaryKey) {
                definition.append("PRIMARY KEY ");
            } else if (isUniqueConstraint) {
                definition.append("UNIQUE ");
            } else {
                definition.append("INDEX ").append(
                        MsDiffUtils.quoteName(index.getString("name"))).append(' ');
            }

            if (!isClustered) {
                definition.append("NON");
            }

            definition.append("CLUSTERED ");

            if (bucketCount > 0) {
                definition.append("HASH");
            }

            definition.append('\n');

            List<String> columns = new ArrayList<>();

            for (XmlReader col : XmlReader.readXML(index.getString("cols"))) {
                boolean isDesc = col.getBoolean("is_desc");
                String colName = col.getString("name");
                columns.add(MsDiffUtils.quoteName(colName) + (isDesc ? " DESC" : " ASC"));
            }

            definition.append("(\n\t");
            definition.append(String.join(",\n\t", columns));
            definition.append("\n)");

            if (filter != null) {
                // index only, broken in dump
                definition.append(" WHERE ").append(filter);
            }

            if (isIgnoreDupKey) {
                // constraint only
                definition.append(" WITH (IGNORE_DUP_KEY = ON)");
            } else if (bucketCount > 0) {
                if (filter != null) {
                    definition.append('\n');
                } else {
                    definition.append(" ");
                }
                definition.append("WITH ( BUCKET_COUNT = ").append(bucketCount).append(')');
            }

            if (isPrimaryKey || isUniqueConstraint) {
                type.addConstraint(definition.toString());
            } else {
                type.addIndex(definition.toString());
            }
        }
    }

    private void addChecks(MsType type, List<XmlReader> checks) {
        for (XmlReader check : checks) {
            type.addConstraint("CHECK (" + check.getString("def") + ')');
        }
    }
}
