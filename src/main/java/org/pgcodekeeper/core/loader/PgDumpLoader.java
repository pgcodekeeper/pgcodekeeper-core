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
 *
 * Copyright 2006 StartNet s.r.o.
 *
 * Distributed under MIT license
 *******************************************************************************/
package org.pgcodekeeper.core.loader;

import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.base.AntlrParser;
import org.pgcodekeeper.core.parsers.antlr.base.AntlrTask;
import org.pgcodekeeper.core.parsers.antlr.ch.ChSQLOverridesListener;
import org.pgcodekeeper.core.parsers.antlr.ch.ChSqlContextProcessor;
import org.pgcodekeeper.core.parsers.antlr.ch.CustomChSQLParserListener;
import org.pgcodekeeper.core.parsers.antlr.ms.CustomTSQLParserListener;
import org.pgcodekeeper.core.parsers.antlr.ms.TSQLOverridesListener;
import org.pgcodekeeper.core.parsers.antlr.ms.TSqlContextProcessor;
import org.pgcodekeeper.core.parsers.antlr.pg.CustomSQLParserListener;
import org.pgcodekeeper.core.parsers.antlr.pg.SQLOverridesListener;
import org.pgcodekeeper.core.parsers.antlr.pg.SqlContextProcessor;
import org.pgcodekeeper.core.schema.*;
import org.pgcodekeeper.core.schema.ch.ChDatabase;
import org.pgcodekeeper.core.schema.ch.ChSchema;
import org.pgcodekeeper.core.schema.ms.MsDatabase;
import org.pgcodekeeper.core.schema.ms.MsSchema;
import org.pgcodekeeper.core.schema.pg.PgDatabase;
import org.pgcodekeeper.core.schema.pg.PgSchema;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.utils.InputStreamProvider;
import org.pgcodekeeper.core.monitor.NullMonitor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Queue;

/**
 * Database loader for SQL dump files. Parses SQL scripts using ANTLR and
 * builds database schema objects with support for different parsing modes
 * and statement overrides.
 *
 * @author fordfrog
 */
public class PgDumpLoader extends DatabaseLoader {

    private final InputStreamProvider input;
    private final String inputObjectName;
    private final ISettings settings;

    private final IMonitor monitor;
    private final int monitoringLevel;

    private ParserListenerMode mode = ParserListenerMode.NORMAL;
    private Map<PgStatement, StatementOverride> overrides;

    /**
     * Sets the parser listener mode for controlling parsing behavior.
     *
     * @param mode the parser listener mode to set
     */
    public void setMode(ParserListenerMode mode) {
        this.mode = mode;
    }

    /**
     * Sets the statement overrides map for customizing parsed statements.
     *
     * @param overrides map of statement overrides
     */
    public void setOverridesMap(Map<PgStatement, StatementOverride> overrides) {
        this.overrides = overrides;
    }

    /**
     * Creates a new dump loader with full configuration.
     *
     * @param input           the input stream provider for the SQL dump
     * @param inputObjectName name of the input object for error reporting
     * @param settings        loader settings and configuration
     * @param monitor         progress monitor for tracking parsing progress
     * @param monitoringLevel level of progress monitoring detail
     */
    public PgDumpLoader(InputStreamProvider input, String inputObjectName,
                        ISettings settings, IMonitor monitor, int monitoringLevel) {
        this.input = input;
        this.inputObjectName = inputObjectName;
        this.settings = settings;
        this.monitor = monitor;
        this.monitoringLevel = monitoringLevel;
    }

    /**
     * Creates a new dump loader with default monitoring level of 1.
     *
     * @param input           the input stream provider for the SQL dump
     * @param inputObjectName name of the input object for error reporting
     * @param settings        loader settings and configuration
     * @param monitor         progress monitor for tracking parsing progress
     */
    public PgDumpLoader(InputStreamProvider input, String inputObjectName,
                        ISettings settings, IMonitor monitor) {
        this(input, inputObjectName, settings, monitor, 1);
    }

    /**
     * Creates a new dump loader with null progress monitor and no monitoring.
     *
     * @param input           the input stream provider for the SQL dump
     * @param inputObjectName name of the input object for error reporting
     * @param settings        loader settings and configuration
     */
    public PgDumpLoader(InputStreamProvider input, String inputObjectName, ISettings settings) {
        this(input, inputObjectName, settings, new NullMonitor(), 0);
    }

    /**
     * This constructor creates {@link InputStreamProvider} using inputFile parameter.
     */
    public PgDumpLoader(Path inputFile, ISettings settings, IMonitor monitor, int monitoringLevel) {
        this(() -> Files.newInputStream(inputFile), inputFile.toString(), settings, monitor, monitoringLevel);
    }

    /**
     * Creates a new dump loader for a file path with default monitoring level of 1.
     *
     * @param inputFile the path to the SQL dump file
     * @param settings  loader settings and configuration
     * @param monitor   progress monitor for tracking parsing progress
     */
    public PgDumpLoader(Path inputFile, ISettings settings, IMonitor monitor) {
        this(inputFile, settings, monitor, 1);
    }

    /**
     * Creates a new dump loader for a file path with null progress monitor and no monitoring.
     *
     * @param inputFile the path to the SQL dump file
     * @param settings  loader settings and configuration
     */
    public PgDumpLoader(Path inputFile, ISettings settings) {
        this(inputFile, settings, new NullMonitor(), 0);
    }

    @Override
    public AbstractDatabase load() throws IOException, InterruptedException {
        AbstractDatabase d = createDb(settings);
        loadAsync(d, antlrTasks);
        finishLoaders();
        return d;
    }

    /**
     * Loads SQL dump asynchronously into the provided database with default schema setup.
     * Creates and adds a default schema based on database type, then delegates to loadDatabase.
     *
     * @param d          the target database to load into
     * @param antlrTasks queue for managing ANTLR parsing tasks
     * @return the loaded database with parsed SQL objects
     * @throws InterruptedException if parsing is interrupted
     */
    public AbstractDatabase loadAsync(AbstractDatabase d, Queue<AntlrTask<?>> antlrTasks)
            throws InterruptedException {
        AbstractSchema schema = switch (settings.getDbType()) {
            case MS -> new MsSchema(Consts.DBO);
            case PG -> new PgSchema(Consts.PUBLIC);
            case CH -> new ChSchema(Consts.CH_DEFAULT_DB);
        };
        d.addSchema(schema);
        PgObjLocation loc = new PgObjLocation.Builder()
                .setObject(new GenericColumn(schema.getName(), DbObjType.SCHEMA))
                .build();

        schema.setLocation(loc);
        d.setDefaultSchema(schema.getName());
        loadDatabase(d, antlrTasks);
        return d;
    }

    /**
     * Loads SQL dump into the specified database using ANTLR parsing.
     * Selects appropriate parser listener based on database type and override configuration.
     * Supports PostgreSQL, Microsoft SQL Server, and ClickHouse databases.
     *
     * @param intoDb     the target database to load parsed objects into
     * @param antlrTasks queue for managing ANTLR parsing tasks
     * @return the database with loaded SQL objects
     * @throws InterruptedException if parsing is interrupted
     */
    public AbstractDatabase loadDatabase(AbstractDatabase intoDb, Queue<AntlrTask<?>> antlrTasks)
            throws InterruptedException {
        IMonitor.checkCancelled(monitor);
        switch (settings.getDbType()) {
            case PG:
                SqlContextProcessor sqlListener;
                if (overrides != null) {
                    sqlListener = new SQLOverridesListener((PgDatabase) intoDb, inputObjectName, mode, errors, monitor,
                            overrides, settings);
                } else {
                    sqlListener = new CustomSQLParserListener((PgDatabase) intoDb, inputObjectName, mode, errors,
                            antlrTasks, monitor, settings);
                }

                AntlrParser.parseSqlStream(input, settings.getInCharsetName(), inputObjectName, errors,
                        monitor, monitoringLevel, sqlListener, antlrTasks);
                break;
            case MS:
                TSqlContextProcessor tsqlListener;
                if (overrides != null) {
                    tsqlListener = new TSQLOverridesListener((MsDatabase) intoDb, inputObjectName, mode, errors, monitor,
                            overrides, settings);
                } else {
                    tsqlListener = new CustomTSQLParserListener((MsDatabase) intoDb, inputObjectName, mode, errors, monitor,
                            settings);
                }
                AntlrParser.parseTSqlStream(input, settings.getInCharsetName(), inputObjectName, errors,
                        monitor, monitoringLevel, tsqlListener, antlrTasks);
                break;
            case CH:
                ChSqlContextProcessor chSqlListener;
                if (overrides != null) {
                    chSqlListener = new ChSQLOverridesListener((ChDatabase) intoDb, inputObjectName, mode, errors, monitor,
                            overrides, settings);
                } else {
                    chSqlListener = new CustomChSQLParserListener((ChDatabase) intoDb, inputObjectName, mode, errors,
                            monitor, settings);
                }
                AntlrParser.parseChSqlStream(input, settings.getInCharsetName(), inputObjectName, errors,
                        monitor, monitoringLevel, chSqlListener, antlrTasks);
                break;
            default:
                throw new IllegalArgumentException(Messages.DatabaseType_unsupported_type + settings.getDbType());
        }

        return intoDb;
    }
}