package cz.startnet.utils.pgdiff.loader.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import cz.startnet.utils.pgdiff.PgDiffUtils;
import cz.startnet.utils.pgdiff.loader.JdbcTimestampLoader;
import cz.startnet.utils.pgdiff.loader.timestamps.ObjectTimestamp;
import cz.startnet.utils.pgdiff.schema.PgDatabase;
import cz.startnet.utils.pgdiff.schema.PgFunction;
import cz.startnet.utils.pgdiff.schema.PgRule;
import cz.startnet.utils.pgdiff.schema.PgSchema;
import cz.startnet.utils.pgdiff.schema.PgSequence;
import cz.startnet.utils.pgdiff.schema.PgStatement;
import cz.startnet.utils.pgdiff.schema.PgTable;
import cz.startnet.utils.pgdiff.schema.PgTrigger;
import cz.startnet.utils.pgdiff.schema.PgType;
import cz.startnet.utils.pgdiff.schema.PgView;
import cz.startnet.utils.pgdiff.wrappers.JsonResultSetWrapper;
import cz.startnet.utils.pgdiff.wrappers.ResultSetWrapper;
import cz.startnet.utils.pgdiff.wrappers.SQLResultSetWrapper;
import cz.startnet.utils.pgdiff.wrappers.WrapperAccessException;
import ru.taximaxim.codekeeper.apgdiff.Log;
import ru.taximaxim.codekeeper.apgdiff.model.difftree.DbObjType;

public abstract class JdbcReader implements PgCatalogStrings {

    protected final JdbcReaderFactory factory;
    protected final JdbcLoaderBase loader;
    protected final int currentVersion;
    protected final String fallbackQuery;

    protected JdbcReader(JdbcReaderFactory factory, JdbcLoaderBase loader, int currentVersion) {
        this.factory = factory;
        this.loader = loader;
        this.currentVersion = currentVersion;
        this.fallbackQuery = factory.makeFallbackQuery(currentVersion);
    }

    public void read() throws SQLException, InterruptedException, WrapperAccessException {
        boolean helperSuccess = false;
        if ((loader.availableHelpersBits & factory.hasHelperMask) != 0) {
            try {
                readAllUsingHelper();
                helperSuccess = true;
            } catch (SQLException ex) {
                Log.log(Log.LOG_WARNING, "Error trying to use server JDBC helper, "
                        + "falling back to old queries: " + factory.helperFunction, ex);
            }
        }
        if (!helperSuccess) {
            readSchemasSeparately();
        }
    }

    private void readAllUsingHelper() throws SQLException, InterruptedException, WrapperAccessException {
        try (PreparedStatement st = loader.connection.prepareStatement(factory.helperQuery)) {
            loader.setCurrentOperation(factory.helperFunction + " query");

            st.setArray(1, loader.schemas.oids);
            st.setArray(2, loader.schemas.names);
            try (ResultSet result = st.executeQuery()) {
                while (result.next()) {
                    ResultSetWrapper wrapper = new JsonResultSetWrapper(result.getString(1));
                    PgDiffUtils.checkCancelled(loader.monitor);
                    processResult(wrapper, loader.schemas.map.get(result.getLong("schema_oid")));
                }
            }
        }
    }

    private void readSchemasSeparately() throws SQLException, InterruptedException, WrapperAccessException {
        String query = fallbackQuery;
        boolean isTime = loader instanceof JdbcTimestampLoader;
        DbObjType type = getType();
        List<ObjectTimestamp> objects = null;
        PgDatabase projDb = null;

        if (isTime) {
            objects = ((JdbcTimestampLoader)loader).getObjects();
            projDb = ((JdbcTimestampLoader)loader).getProjDb();

            List<Long> oids = objects.stream().filter(obj -> (
                    obj.getType() == (type == DbObjType.CONSTRAINT ? DbObjType.TABLE : type)))
                    .map(ObjectTimestamp::getObjId).collect(Collectors.toList());
            if (!oids.isEmpty()) {
                query = JdbcReaderFactory.excludeObjects(query, oids);
            }
        }

        try (PreparedStatement st = loader.connection.prepareStatement(query)) {
            for (Entry<Long, PgSchema> schema : loader.schemas.map.entrySet()) {
                PgSchema sc = schema.getValue();
                if (isTime) {
                    for (ObjectTimestamp obj: objects) {
                        if (obj.getSchema().equals(sc.getName()) && ((obj.getType() == type)
                                || (obj.getType() == DbObjType.TABLE && type == DbObjType.CONSTRAINT))) {
                            PgStatement statement = obj.getShallowCopy(projDb);
                            switch (type) {
                            case VIEW:
                                sc.addView((PgView) statement);
                                break;
                            case TABLE:
                                sc.addTable((PgTable) statement);
                                break;
                            case RULE:
                                PgRule rule = (PgRule) statement;
                                sc.getRuleContainer(rule.getParent().getName()).addRule(rule);;
                                break;
                            case TRIGGER:
                                PgTrigger trig = (PgTrigger) statement;
                                sc.getTriggerContainer(trig.getParent().getName()).addTrigger(trig);
                                break;
                                /*case INDEX:
                                PgIndex index = (PgIndex) statement;
                                sc.getTable(index.getParent().getName()).addIndex(index);
                                break;*/
                            case FUNCTION:
                                sc.addFunction((PgFunction) statement);
                                break;
                            case CONSTRAINT:
                                PgTable table = (PgTable) obj.getDeepCopy(projDb);
                                PgTable newTable = sc.getTable(table.getName());
                                table.getConstraints().forEach(con -> newTable.addConstraint(con.shallowCopy()));
                                break;
                            case TYPE:
                                sc.addType((PgType) statement);
                                break;
                            case SEQUENCE:
                                sc.addSequence((PgSequence) statement);
                                break;
                            default:
                                break;
                            }
                        }
                    }
                }

                loader.setCurrentOperation("set search_path query");
                loader.statement.execute("SET search_path TO " +
                        PgDiffUtils.getQuotedName(schema.getValue().getName()) + ", pg_catalog;");

                loader.setCurrentOperation(factory.helperFunction + " query for schema " + sc.getName());
                st.setLong(1, schema.getKey());
                try (ResultSet result = st.executeQuery()) {
                    while (result.next()) {
                        ResultSetWrapper wrapper = new SQLResultSetWrapper(result);
                        PgDiffUtils.checkCancelled(loader.monitor);
                        processResult(wrapper, sc);
                    }
                }
            }
        }
    }

    protected abstract void processResult(ResultSetWrapper json, PgSchema schema)
            throws SQLException, WrapperAccessException;

    protected abstract DbObjType getType();

}
