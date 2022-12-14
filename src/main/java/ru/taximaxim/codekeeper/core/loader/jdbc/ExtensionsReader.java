package ru.taximaxim.codekeeper.core.loader.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

import ru.taximaxim.codekeeper.core.PgDiffUtils;
import ru.taximaxim.codekeeper.core.loader.JdbcQueries;
import ru.taximaxim.codekeeper.core.model.difftree.DbObjType;
import ru.taximaxim.codekeeper.core.schema.GenericColumn;
import ru.taximaxim.codekeeper.core.schema.PgDatabase;
import ru.taximaxim.codekeeper.core.schema.PgExtension;

public class ExtensionsReader implements PgCatalogStrings {

    private final JdbcLoaderBase loader;
    private final PgDatabase db;

    public ExtensionsReader(JdbcLoaderBase loader, PgDatabase db) {
        this.loader = loader;
        this.db = db;
    }

    public void read() throws SQLException, InterruptedException {
        loader.setCurrentOperation("extensions query");
        String query = JdbcQueries.QUERY_EXTENSIONS.makeQuery(loader, "pg_extension");

        try (ResultSet res = loader.runner.runScript(loader.statement, query)) {
            while (res.next()) {
                PgDiffUtils.checkCancelled(loader.monitor);
                PgExtension extension = getExtension(res);
                db.addExtension(extension);
                loader.setAuthor(extension, res);
            }
        }
    }

    private PgExtension getExtension(ResultSet res) throws SQLException {
        String extName = res.getString("extname");
        loader.setCurrentObject(new GenericColumn(extName, DbObjType.EXTENSION));
        PgExtension e = new PgExtension(extName);
        e.setSchema(res.getString("namespace"));
        e.setRelocatable(res.getBoolean("extrelocatable"));
        e.addDep(new GenericColumn(e.getSchema(), DbObjType.SCHEMA));

        String comment = res.getString("description");
        if (comment != null && !comment.isEmpty()) {
            e.setComment(loader.args, PgDiffUtils.quoteString(comment));
        }
        return e;
    }
}
