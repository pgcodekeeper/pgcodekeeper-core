package cz.startnet.utils.pgdiff.loader.jdbc;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;

import cz.startnet.utils.pgdiff.PgDiffUtils;
import cz.startnet.utils.pgdiff.parsers.antlr.expr.ValueExpr;
import cz.startnet.utils.pgdiff.parsers.antlr.rulectx.Vex;
import cz.startnet.utils.pgdiff.parsers.antlr.statements.ParserAbstract;
import cz.startnet.utils.pgdiff.schema.GenericColumn;
import cz.startnet.utils.pgdiff.schema.PgColumn;
import cz.startnet.utils.pgdiff.schema.PgSchema;
import cz.startnet.utils.pgdiff.schema.PgTable;
import ru.taximaxim.codekeeper.apgdiff.model.difftree.DbObjType;

public class TablesReader extends JdbcReader {

    public static class TablesReaderFactory extends JdbcReaderFactory {

        public TablesReaderFactory(long hasHelperMask, String helperFunction, String fallbackQuery) {
            super(hasHelperMask, helperFunction, fallbackQuery);
        }

        @Override
        public JdbcReader getReader(JdbcLoaderBase loader) {
            return new TablesReader(this, loader);
        }
    }

    private TablesReader(JdbcReaderFactory factory, JdbcLoaderBase loader) {
        super(factory, loader);
    }

    @Override
    protected void processResult(ResultSet result, PgSchema schema) throws SQLException {
        PgTable table = getTable(result, schema.getName());
        loader.monitor.worked(1);
        if (table != null) {
            schema.addTable(table);
        }
    }

    private PgTable getTable(ResultSet res, String schemaName) throws SQLException {
        String tableName = res.getString(CLASS_RELNAME);
        loader.setCurrentObject(new GenericColumn(schemaName, tableName, DbObjType.TABLE));
        PgTable t = new PgTable(tableName, "");

        // PRIVILEGES, OWNER
        loader.setOwner(t, res.getLong(CLASS_RELOWNER));
        loader.setPrivileges(t, PgDiffUtils.getQuotedName(t.getName()), res.getString("aclarray"), t.getOwner(), null);

        Integer[] colNumbers = (Integer[]) res.getArray("col_numbers").getArray();
        String[] colNames = (String[]) res.getArray("col_names").getArray();
        Long[] colTypeIds = (Long[]) res.getArray("col_type_ids").getArray();
        String[] colTypeName = (String[]) res.getArray("col_type_name").getArray();
        String[] colDefaults = (String[]) res.getArray("col_defaults").getArray();
        String[] colComments = (String[]) res.getArray("col_comments").getArray();
        Boolean[] colNotNull = (Boolean[]) res.getArray("col_notnull").getArray();
        Integer[] colStatictics = (Integer[]) res.getArray("col_statictics").getArray();
        Boolean[] colIsLocal = (Boolean[]) res.getArray("col_local").getArray();
        Long[] colCollation = (Long[]) res.getArray("col_collation").getArray();
        Long[] colTypCollation = (Long[]) res.getArray("col_typcollation").getArray();
        String[] colCollationName = (String[]) res.getArray("col_collationname").getArray();
        String[] colCollationSchema = (String[]) res.getArray("col_collationnspname").getArray();
        String[] colAcl = (String[]) res.getArray("col_acl").getArray();

        for (int i = 0; i < colNumbers.length; i++) {
            if (colNumbers[i] < 1 || !colIsLocal[i]) {
                // пропускать не локальные (Inherited)  и системные (System) колонки
                continue;
            }

            PgColumn column = new PgColumn(colNames[i]);
            column.setType(colTypeName[i]);
            loader.cachedTypesByOid.get(colTypeIds[i]).addTypeDepcy(column);

            // unbox
            long collation = colCollation[i];
            if (collation != 0 && collation != colTypCollation[i]) {
                column.setCollation(PgDiffUtils.getQuotedName(colCollationSchema[i])
                        + '.' + PgDiffUtils.getQuotedName(colCollationName[i]));
            }

            String columnDefault = colDefaults[i];
            if (columnDefault != null && !columnDefault.isEmpty()) {
                column.setDefaultValue(columnDefault);
                loader.submitAntlrTask(columnDefault, p -> {
                    ValueExpr vex = new ValueExpr(schemaName);
                    vex.analyze(new Vex(p.vex_eof().vex()));
                    return vex.getDepcies();
                }, column::addAllDeps);
            }

            if (colNotNull[i]) {
                column.setNullValue(false);
            }

            int statistics = colStatictics[i];
            // if the attstattarget entry for this column is
            // non-negative (i.e. it's not the default value)
            if (statistics > -1) {
                column.setStatistics(statistics);
            }

            String comment = colComments[i];
            if (comment != null && !comment.isEmpty()) {
                column.setComment(loader.args, PgDiffUtils.quoteString(comment));
            }

            // COLUMNS PRIVILEGES
            String columnPrivileges = colAcl[i];
            if (columnPrivileges != null && !columnPrivileges.isEmpty()) {
                loader.setPrivileges(column, PgDiffUtils.getQuotedName(tableName),
                        columnPrivileges, t.getOwner(), PgDiffUtils.getQuotedName(colNames[i]));
            }

            t.addColumn(column);
        }

        // INHERITS
        Array inhrelsarray = res.getArray("inhrelnames");
        if (inhrelsarray != null) {
            String[] inhrelnames = (String[]) inhrelsarray.getArray();
            String[] inhnspnames = (String[]) res.getArray("inhnspnames").getArray();

            for (int i = 0; i < inhrelnames.length; ++i) {
                t.addInherits(schemaName.equals(inhnspnames[i]) ? null : inhnspnames[i], inhrelnames[i]);
                t.addDep(new GenericColumn(inhnspnames[i], inhrelnames[i], DbObjType.TABLE));
            }
        }

        // STORAGE PARAMETERS
        Array arr = res.getArray("reloptions");
        if (arr != null) {
            String[] options = (String[]) arr.getArray();
            ParserAbstract.fillStorageParams(options, t, false);
        }

        arr = res.getArray("toast_reloptions");
        if (arr != null) {
            String[] options = (String[]) arr.getArray();
            ParserAbstract.fillStorageParams(options, t, true);
        }

        if (res.getBoolean("has_oids")){
            t.setHasOids(true);
        }

        // Table COMMENTS
        String comment = res.getString("table_comment");
        if (comment != null && !comment.isEmpty()) {
            t.setComment(loader.args, PgDiffUtils.quoteString(comment));
        }

        // TableSpace
        String tableSpace = res.getString("table_space");
        if (tableSpace != null && !tableSpace.isEmpty()) {
            t.setTablespace(tableSpace);
        }
        return t;
    }
}
