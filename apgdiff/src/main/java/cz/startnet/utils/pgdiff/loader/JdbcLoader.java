package cz.startnet.utils.pgdiff.loader;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;

import org.eclipse.core.runtime.SubMonitor;

import cz.startnet.utils.pgdiff.PgDiffArguments;
import cz.startnet.utils.pgdiff.PgDiffUtils;
import cz.startnet.utils.pgdiff.loader.jdbc.ExtensionsReader;
import cz.startnet.utils.pgdiff.loader.jdbc.JdbcLoaderBase;
import cz.startnet.utils.pgdiff.loader.jdbc.JdbcReaderFactory;
import cz.startnet.utils.pgdiff.loader.jdbc.SchemasContainer;
import cz.startnet.utils.pgdiff.loader.jdbc.SchemasReader;
import cz.startnet.utils.pgdiff.loader.jdbc.SequencesReader;
import cz.startnet.utils.pgdiff.loader.jdbc.TimestampsReader;
import cz.startnet.utils.pgdiff.loader.timestamps.DBTimestamp;
import cz.startnet.utils.pgdiff.loader.timestamps.DBTimestampPair;
import cz.startnet.utils.pgdiff.schema.PgDatabase;
import ru.taximaxim.codekeeper.apgdiff.Log;
import ru.taximaxim.codekeeper.apgdiff.localizations.Messages;

public class JdbcLoader extends JdbcLoaderBase {

    public JdbcLoader(JdbcConnector connector, PgDiffArguments pgDiffArguments) {
        this(connector, pgDiffArguments, SubMonitor.convert(null));
    }

    public JdbcLoader(JdbcConnector connector, PgDiffArguments pgDiffArguments,
            SubMonitor monitor) {
        super(connector, monitor, pgDiffArguments);
    }

    public List<String> getErrors() {
        return Collections.unmodifiableList(errors);
    }

    public void setTimeParams(PgDatabase projDB, Path path, String schema) {
        this.projDB = projDB;
        this.schema = schema;
        this.path = path;
    }

    public DBTimestampPair getDbPair() {
        return pair;
    }

    public PgDatabase getDbFromJdbc() throws IOException, InterruptedException {
        PgDatabase d = new PgDatabase(false);
        d.setArguments(args);

        Log.log(Log.LOG_INFO, "Reading db using JDBC.");
        setCurrentOperation("connection setup");
        try (Connection connection = connector.getConnection();
                Statement statement = connection.createStatement()) {
            this.connection = connection;
            this.statement = statement;
            connection.setAutoCommit(false);
            statement.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY");
            statement.execute("SET timezone = " + PgDiffUtils.quoteString(connector.getTimezone()));

            queryCheckVersion();
            queryTypesForCache();
            queryRoles();
            setupMonitorWork();

            if (schema != null) {
                DBTimestamp projTime = DBTimestamp.getDBTimestamp(path);
                DBTimestamp dbTime = new TimestampsReader(this).read();
                finishAntlr();
                pair = new DBTimestampPair(projTime, dbTime);
                objects = pair.searchMatch();
            }

            schemas = new SchemasReader(this, d).read();
            try (SchemasContainer schemas = this.schemas) {
                availableHelpersBits = useServerHelpers ? JdbcReaderFactory.getAvailableHelpersBits(this) : 0;
                for (JdbcReaderFactory f : JdbcReaderFactory.FACTORIES) {
                    f.getReader(this).read();
                }
                new ExtensionsReader(this, d).read();

                if(!SupportedVersion.VERSION_10.checkVersion(version)) {
                    SequencesReader.querySequencesData(d, this);
                }
            }
            connection.commit();
            finishAntlr();
            d.sortColumns();
            Log.log(Log.LOG_INFO, "Database object has been successfully queried from JDBC");
        } catch (InterruptedException ex) {
            throw ex;
        } catch (Exception e) {
            // connection is closed at this point, trust Postgres to rollback it; we're a read-only xact anyway
            throw new IOException(MessageFormat.format(Messages.Connection_DatabaseJdbcAccessError,
                    e.getLocalizedMessage(), getCurrentLocation()), e);
        }
        return d;
    }
}
