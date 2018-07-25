package cz.startnet.utils.pgdiff.loader.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import cz.startnet.utils.pgdiff.MsDiffUtils;
import cz.startnet.utils.pgdiff.loader.SupportedVersion;
import cz.startnet.utils.pgdiff.schema.GenericColumn;
import cz.startnet.utils.pgdiff.schema.MsColumn;
import cz.startnet.utils.pgdiff.schema.PgSchema;
import cz.startnet.utils.pgdiff.schema.SimpleMsTable;
import cz.startnet.utils.pgdiff.wrappers.WrapperAccessException;
import ru.taximaxim.codekeeper.apgdiff.ApgdiffConsts;
import ru.taximaxim.codekeeper.apgdiff.model.difftree.DbObjType;

public class MsTablesReader extends JdbcMsReader {


    public static class MsTablesReaderFactory extends JdbcReaderFactory {

        public MsTablesReaderFactory(Map<SupportedVersion, String> queries) {
            super(0, "", queries);
        }

        @Override
        public JdbcReader getReader(JdbcLoaderBase loader) {
            return new MsTablesReader(this, loader);
        }
    }

    public MsTablesReader(JdbcReaderFactory factory, JdbcLoaderBase loader) {
        super(factory, loader);
    }

    @Override
    protected void processResult(ResultSet res, PgSchema schema) throws SQLException, WrapperAccessException {
        loader.monitor.worked(1);
        String tableName = res.getString("name");
        loader.setCurrentObject(new GenericColumn(schema.getName(), tableName, DbObjType.TABLE));
        SimpleMsTable table = new SimpleMsTable(tableName, "");

        if (!loader.args.isIgnorePrivileges()) {
            String owner = res.getString("owner");
            table.setOwner(owner == null ? ApgdiffConsts.SCHEMA_OWNER : owner);
        }

        if (res.getBoolean("is_memory_optimized")) {
            table.addOption("MEMORY_OPTIMIZED" , "ON");
        }

        if (res.getBoolean("durability")) {
            table.addOption("DURABILITY", res.getString("durability_desc"));
        }

        table.setTextImage(res.getString("text_image"));
        table.setFileStream(res.getString("file_stream"));
        table.setAnsiNulls(res.getBoolean("uses_ansi_nulls"));

        for (JsonReader col : JsonReader.fromArray(res.getString("cols"))) {
            MsColumn column = new MsColumn(col.getString("name"));
            String argSize = "";
            String dataType = col.getString("type");
            int size = col.getInt("size");
            if (dataType.endsWith("varchar")) {
                argSize = size == -1 ? "(max)" : ("(" + size + ")");
            } else if ("decimal".equals(dataType)) {
                argSize = "(" + col.getInt("pr") + ',' + col.getInt("sc") + ')';
            }
            // TODO other type with size

            column.setType(MsDiffUtils.quoteName(dataType) + argSize);
            column.setSparse(col.getBoolean("sp"));
            column.setNullValue(col.getBoolean("nl"));
            if (col.getBoolean("ii")) {
                column.setIdentity(Integer.toString(col.getInt("s")), Integer.toString(col.getInt("i")));
                column.setNotForRep(col.getBoolean("nfr"));
            }

            String def = col.getString("dv");
            if (def != null) {
                column.setDefaultValue(def);
                column.setDefaultName(col.getString("dn"));
            }

            column.setCollation(col.getString("cn"));

            column.setExpression(col.getString("def"));
            table.addColumn(column);
        }


        // loader.setPrivileges(s, res.getString("aclarray"));

        // TODO add filegroup/tablespace
        // table.setTablespace(tablespace);

        schema.addTable(table);
    }

    @Override
    protected DbObjType getType() {
        return DbObjType.TABLE;
    }
}
