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
package org.pgcodekeeper.core.loader.ms;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.text.MessageFormat;

import org.eclipse.core.runtime.SubMonitor;
import org.pgcodekeeper.core.loader.AbstractJdbcConnector;
import org.pgcodekeeper.core.loader.jdbc.JdbcLoaderBase;
import org.pgcodekeeper.core.loader.jdbc.ms.MsAssembliesReader;
import org.pgcodekeeper.core.loader.jdbc.ms.MsCheckConstraintsReader;
import org.pgcodekeeper.core.loader.jdbc.ms.MsExtendedObjectsReader;
import org.pgcodekeeper.core.loader.jdbc.ms.MsFKReader;
import org.pgcodekeeper.core.loader.jdbc.ms.MsFPVTReader;
import org.pgcodekeeper.core.loader.jdbc.ms.MsIndicesAndPKReader;
import org.pgcodekeeper.core.loader.jdbc.ms.MsRolesReader;
import org.pgcodekeeper.core.loader.jdbc.ms.MsSchemasReader;
import org.pgcodekeeper.core.loader.jdbc.ms.MsSequencesReader;
import org.pgcodekeeper.core.loader.jdbc.ms.MsStatisticsReader;
import org.pgcodekeeper.core.loader.jdbc.ms.MsTablesReader;
import org.pgcodekeeper.core.loader.jdbc.ms.MsTypesReader;
import org.pgcodekeeper.core.loader.jdbc.ms.MsUsersReader;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.model.difftree.IgnoreSchemaList;
import org.pgcodekeeper.core.schema.AbstractDatabase;
import org.pgcodekeeper.core.schema.ms.MsDatabase;
import org.pgcodekeeper.core.settings.ISettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JdbcMsLoader extends JdbcLoaderBase {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcMsLoader.class);

    public JdbcMsLoader(AbstractJdbcConnector connector, ISettings settings,
            SubMonitor monitor, IgnoreSchemaList ignoreSchemaList) {
        super(connector, monitor, settings, ignoreSchemaList);
    }

    @Override
    public AbstractDatabase load() throws IOException, InterruptedException {
        MsDatabase d = (MsDatabase) createDb(getSettings());

        LOG.info(Messages.JdbcLoader_log_reading_db_jdbc);
        setCurrentOperation(Messages.JdbcChLoader_log_connection_db);
        try (Connection connection = connector.getConnection();
                Statement statement = connection.createStatement()) {
            this.connection = connection;
            this.statement = statement;

            connection.setAutoCommit(false);
            // TODO maybe not needed and/or may cause extra locking (compared to PG)
            // may need to be removed, Source Control seems to work in default READ COMMITTED state
            getRunner().run(statement, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");

            // TODO add counting objects later
            //setupMonitorWork();

            queryCheckMsVersion();

            LOG.info(Messages.JdbcLoader_log_read_db_objects);
            new MsSchemasReader(this, d).read();
            new MsTablesReader(this).read();
            new MsFPVTReader(this).read();
            new MsExtendedObjectsReader(this).read();
            new MsSequencesReader(this).read();
            new MsIndicesAndPKReader(this).read();
            new MsFKReader(this).read();
            new MsCheckConstraintsReader(this).read();
            new MsTypesReader(this).read();
            new MsAssembliesReader(this, d).read();
            new MsRolesReader(this, d).read();
            new MsUsersReader(this, d).read();
            new MsStatisticsReader(this).read();

            finishLoaders();

            connection.commit();

            LOG.info(Messages.JdbcLoader_log_succes_queried);
        } catch (InterruptedException ex) {
            throw ex;
        } catch (Exception e) {
            // connection is closed at this point
            throw new IOException(MessageFormat.format(Messages.Connection_DatabaseJdbcAccessError,
                    e.getLocalizedMessage(), getCurrentLocation()), e);
        }
        return d;
    }
}
