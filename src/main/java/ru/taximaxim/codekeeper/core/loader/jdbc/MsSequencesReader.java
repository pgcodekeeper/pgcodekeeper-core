package ru.taximaxim.codekeeper.core.loader.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

import ru.taximaxim.codekeeper.core.loader.JdbcQueries;
import ru.taximaxim.codekeeper.core.model.difftree.DbObjType;
import ru.taximaxim.codekeeper.core.schema.AbstractSchema;
import ru.taximaxim.codekeeper.core.schema.GenericColumn;
import ru.taximaxim.codekeeper.core.schema.MsSequence;

public class MsSequencesReader extends JdbcReader {

    public MsSequencesReader(JdbcLoaderBase loader) {
        super(JdbcQueries.QUERY_MS_SEQUENCES, loader);
    }

    @Override
    protected void processResult(ResultSet res, AbstractSchema schema) throws SQLException, XmlReaderException {
        loader.monitor.worked(1);
        String sequenceName = res.getString("name");
        loader.setCurrentObject(new GenericColumn(schema.getName(), sequenceName, DbObjType.SEQUENCE));
        MsSequence s = new MsSequence(sequenceName);

        String dataType = res.getString("data_type");
        int precision = res.getInt("precision");

        s.setDataType(JdbcLoaderBase.getMsType(s, res.getString("type_schema"),
                dataType, res.getBoolean("is_user_defined"), 0, precision, 0, false));

        s.setStartWith(Long.toString(res.getLong("start_value")));
        s.setMinMaxInc(res.getLong("increment"), res.getLong("maximum_value"),
                res.getLong("minimum_value"), dataType, precision);
        s.setCached(res.getBoolean("is_cached"));

        // getInt convert null to 0
        Object cashe = res.getObject("cache_size");
        if (cashe != null) {
            s.setCache(cashe.toString());
        }

        s.setCycle(res.getBoolean("is_cycling"));

        loader.setOwner(s, res.getString("owner"));

        schema.addSequence(s);
        loader.setPrivileges(s, XmlReader.readXML(res.getString("acl")));
    }
}