package ru.taximaxim.codekeeper.core.loader.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.antlr.v4.runtime.CommonTokenStream;

import ru.taximaxim.codekeeper.core.PgDiffUtils;
import ru.taximaxim.codekeeper.core.loader.JdbcQueries;
import ru.taximaxim.codekeeper.core.loader.SupportedVersion;
import ru.taximaxim.codekeeper.core.model.difftree.DbObjType;
import ru.taximaxim.codekeeper.core.parsers.antlr.AntlrUtils;
import ru.taximaxim.codekeeper.core.parsers.antlr.expr.launcher.VexAnalysisLauncher;
import ru.taximaxim.codekeeper.core.parsers.antlr.expr.launcher.ViewAnalysisLauncher;
import ru.taximaxim.codekeeper.core.parsers.antlr.statements.ParserAbstract;
import ru.taximaxim.codekeeper.core.schema.AbstractSchema;
import ru.taximaxim.codekeeper.core.schema.AbstractView;
import ru.taximaxim.codekeeper.core.schema.GenericColumn;
import ru.taximaxim.codekeeper.core.schema.PgDatabase;
import ru.taximaxim.codekeeper.core.schema.PgView;
import ru.taximaxim.codekeeper.core.utils.Pair;

public class ViewsReader extends JdbcReader {

    public ViewsReader(JdbcLoaderBase loader) {
        super(JdbcQueries.QUERY_VIEWS, loader);
    }

    @Override
    protected void processResult(ResultSet result, AbstractSchema schema) throws SQLException {
        AbstractView view = getView(result, schema);
        loader.monitor.worked(1);
        schema.addView(view);
    }

    private AbstractView getView(ResultSet res, AbstractSchema schema) throws SQLException {
        String schemaName = schema.getName();
        String viewName = res.getString(CLASS_RELNAME);
        loader.setCurrentObject(new GenericColumn(schemaName, viewName, DbObjType.VIEW));

        PgView v = new PgView(viewName);

        // materialized view
        if ("m".equals(res.getString("kind"))) {
            v.setIsWithData(res.getBoolean("relispopulated"));
            String tableSpace = res.getString("table_space");
            if (tableSpace != null && !tableSpace.isEmpty()) {
                v.setTablespace(tableSpace);
            }
            if (SupportedVersion.VERSION_12.isLE(loader.version)) {
                v.setMethod(res.getString("access_method"));
            }
        }

        String definition = res.getString("definition");
        checkObjectValidity(definition, DbObjType.VIEW, viewName);
        String viewDef = definition.trim();
        int semicolonPos = viewDef.length() - 1;
        String query = viewDef.charAt(semicolonPos) == ';' ? viewDef.substring(0, semicolonPos) : viewDef;

        PgDatabase dataBase = schema.getDatabase();

        loader.submitAntlrTask(viewDef,
                p -> new Pair<>(
                        p.sql().statement(0).data_statement().select_stmt(),
                        (CommonTokenStream) p.getTokenStream()),
                pair -> {
                    dataBase.addAnalysisLauncher(new ViewAnalysisLauncher(
                            v, pair.getFirst(), loader.getCurrentLocation()));
                    v.setQuery(query, AntlrUtils.normalizeWhitespaceUnquoted(
                            pair.getFirst(), pair.getSecond()));
                });

        // OWNER
        loader.setOwner(v, res.getLong(CLASS_RELOWNER));

        // Query columns default values and comments
        String[] colNames = getColArray(res, "column_names");
        if (colNames != null) {
            String[] colComments = getColArray(res, "column_comments");
            String[] colDefaults = getColArray(res, "column_defaults");
            String[] colACLs = getColArray(res, "column_acl");

            for (int i = 0; i < colNames.length; i++) {
                String colName = colNames[i];
                String colDefault = colDefaults[i];
                if (colDefault != null) {
                    v.addColumnDefaultValue(colName, colDefault);
                    loader.submitAntlrTask(colDefault, p -> p.vex_eof().vex().get(0),
                            ctx -> dataBase.addAnalysisLauncher(
                                    new VexAnalysisLauncher(v, ctx, loader.getCurrentLocation())));
                }
                String colComment = colComments[i];
                if (colComment != null) {
                    v.addColumnComment(loader.args, colName, PgDiffUtils.quoteString(colComment));
                }
                String colAcl = colACLs[i];
                // ???????????????????? ???? ?????????????? view ???????????????????????? ?? ???????? view
                if (colAcl != null) {
                    loader.setPrivileges(v, colAcl, colName, schemaName);
                }
            }
        }

        // Query view privileges
        loader.setPrivileges(v, res.getString("relacl"), schemaName);
        loader.setAuthor(v, res);

        // STORAGE PARAMETRS
        String[] options = getColArray(res, "reloptions");
        if (options != null) {
            ParserAbstract.fillOptionParams(options, v::addOption, false, false, false);
        }

        // COMMENT
        String comment = res.getString("comment");
        if (comment != null && !comment.isEmpty()) {
            v.setComment(loader.args, PgDiffUtils.quoteString(comment));
        }

        return v;
    }

    @Override
    protected void setParams(PreparedStatement statement) throws SQLException {
        statement.setBoolean(1, loader.args.isSimplifyView());
    }

    @Override
    protected String getClassId() {
        return "pg_class";
    }
}
