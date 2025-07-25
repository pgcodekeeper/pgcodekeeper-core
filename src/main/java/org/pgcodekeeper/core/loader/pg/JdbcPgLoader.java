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
package org.pgcodekeeper.core.loader.pg;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.MessageFormat;

import org.eclipse.core.runtime.SubMonitor;
import org.pgcodekeeper.core.PgDiffUtils;
import org.pgcodekeeper.core.loader.AbstractJdbcConnector;
import org.pgcodekeeper.core.loader.JdbcQueries;
import org.pgcodekeeper.core.loader.jdbc.JdbcLoaderBase;
import org.pgcodekeeper.core.loader.jdbc.pg.*;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.model.difftree.IgnoreSchemaList;
import org.pgcodekeeper.core.schema.pg.PgDatabase;
import org.pgcodekeeper.core.settings.ISettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JdbcPgLoader extends JdbcLoaderBase {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcPgLoader.class);
    private final String timezone;

    public JdbcPgLoader(AbstractJdbcConnector connector, String timezone, ISettings settings,
            SubMonitor monitor, IgnoreSchemaList ignoreSchemaList) {
        super(connector, monitor, settings, ignoreSchemaList);
        this.timezone = timezone;
    }

    @Override
    public PgDatabase load() throws IOException, InterruptedException {
        PgDatabase d = (PgDatabase) createDb(getSettings());

        LOG.info(Messages.JdbcLoader_log_reading_db_jdbc);
        setCurrentOperation(Messages.JdbcChLoader_log_connection_db);
        try (Connection connection = connector.getConnection();
                Statement statement = connection.createStatement()) {
            this.connection = connection;
            this.statement = statement;
            connection.setAutoCommit(false);
            getRunner().run(statement, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY");
            getRunner().run(statement, "SET search_path TO pg_catalog;");
            getRunner().run(statement, "SET timezone = " + PgDiffUtils.quoteString(timezone));

            queryCheckPgVersion();
            queryCheckGreenplumDb();
            queryCheckLastSysOid();
            queryTypesForCache();
            queryRoles();
            queryCheckExtension();
            setupMonitorWork();

            LOG.info(Messages.JdbcLoader_log_read_db_objects);
            new SchemasReader(this, d).read();

            // NOTE: order of readers has been changed to move the heaviest ANTLR tasks to the beginning
            // to give them a chance to finish while JDBC processes other non-ANTLR stuff
            new FunctionsReader(this).read();
            new ViewsReader(this).read();
            new TablesReader(this).read();
            new RulesReader(this).read();
            if (SupportedPgVersion.VERSION_9_5.isLE(getVersion())) {
                new PoliciesReader(this).read();
            }
            new TriggersReader(this).read();
            new IndicesReader(this).read();
            new ConstraintsReader(this).read();
            new TypesReader(this).read();
            if (SupportedPgVersion.VERSION_10.isLE(getVersion())) {
                new StatisticsReader(this).read();
            }

            // non-ANTLR tasks
            new SequencesReader(this).read();
            new FtsParsersReader(this).read();
            new FtsTemplatesReader(this).read();
            new FtsDictionariesReader(this).read();
            new FtsConfigurationsReader(this).read();
            new OperatorsReader(this).read();

            new ExtensionsReader(this, d).read();
            new EventTriggersReader(this, d).read();
            new CastsReader(this, d).read();
            new ForeignDataWrappersReader(this, d).read();
            new ServersReader(this, d).read();
            try (ResultSet res = getRunner().runScript(statement, JdbcQueries.QUERY_CHECK_USER_PRIVILEGES)) {
                if (res.next() && res.getBoolean("result")) {
                    new UserMappingReader(this, d).read();
                }
            }
            new CollationsReader(this).read();

            if (!SupportedPgVersion.VERSION_10.isLE(getVersion())) {
                SequencesReader.querySequencesData(d, this);
            }
            connection.commit();
            finishLoaders();

            d.sortColumns();

            d.setVersion(SupportedPgVersion.valueOf(getVersion()));
            LOG.info(Messages.JdbcLoader_log_succes_queried);
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
