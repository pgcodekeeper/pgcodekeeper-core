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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Queue;

import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.core.runtime.NullProgressMonitor;
import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.PgDiffUtils;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.AntlrParser;
import org.pgcodekeeper.core.parsers.antlr.AntlrTask;
import org.pgcodekeeper.core.parsers.antlr.ChSQLOverridesListener;
import org.pgcodekeeper.core.parsers.antlr.CustomChSQLParserListener;
import org.pgcodekeeper.core.parsers.antlr.CustomSQLParserListener;
import org.pgcodekeeper.core.parsers.antlr.CustomTSQLParserListener;
import org.pgcodekeeper.core.parsers.antlr.SQLOverridesListener;
import org.pgcodekeeper.core.parsers.antlr.TSQLOverridesListener;
import org.pgcodekeeper.core.parsers.antlr.AntlrContextProcessor.ChSqlContextProcessor;
import org.pgcodekeeper.core.parsers.antlr.AntlrContextProcessor.SqlContextProcessor;
import org.pgcodekeeper.core.parsers.antlr.AntlrContextProcessor.TSqlContextProcessor;
import org.pgcodekeeper.core.schema.AbstractDatabase;
import org.pgcodekeeper.core.schema.AbstractSchema;
import org.pgcodekeeper.core.schema.GenericColumn;
import org.pgcodekeeper.core.schema.PgObjLocation;
import org.pgcodekeeper.core.schema.PgStatement;
import org.pgcodekeeper.core.schema.StatementOverride;
import org.pgcodekeeper.core.schema.ch.ChDatabase;
import org.pgcodekeeper.core.schema.ch.ChSchema;
import org.pgcodekeeper.core.schema.ms.MsDatabase;
import org.pgcodekeeper.core.schema.ms.MsSchema;
import org.pgcodekeeper.core.schema.pg.PgDatabase;
import org.pgcodekeeper.core.schema.pg.PgSchema;
import org.pgcodekeeper.core.settings.ISettings;
import org.pgcodekeeper.core.utils.InputStreamProvider;

/**
 * Loads PostgreSQL dump into classes.
 *
 * @author fordfrog
 */
public class PgDumpLoader extends DatabaseLoader {

    private final InputStreamProvider input;
    private final String inputObjectName;
    private final ISettings settings;

    private final IProgressMonitor monitor;
    private final int monitoringLevel;

    private ParserListenerMode mode = ParserListenerMode.NORMAL;
    private Map<PgStatement, StatementOverride> overrides;

    public void setMode(ParserListenerMode mode) {
        this.mode = mode;
    }

    public void setOverridesMap(Map<PgStatement, StatementOverride> overrides) {
        this.overrides = overrides;
    }

    public PgDumpLoader(InputStreamProvider input, String inputObjectName,
            ISettings settings, IProgressMonitor monitor, int monitoringLevel) {
        this.input = input;
        this.inputObjectName = inputObjectName;
        this.settings = settings;
        this.monitor = monitor;
        this.monitoringLevel = monitoringLevel;
    }

    /**
     * This constructor sets the monitoring level to the default of 1.
     */
    public PgDumpLoader(InputStreamProvider input, String inputObjectName,
            ISettings settings, IProgressMonitor monitor) {
        this(input, inputObjectName, settings, monitor, 1);
    }

    /**
     * This constructor uses {@link NullProgressMonitor}.
     */
    public PgDumpLoader(InputStreamProvider input, String inputObjectName, ISettings settings) {
        this(input, inputObjectName, settings, new NullProgressMonitor(), 0);
    }

    /**
     * This constructor creates {@link InputStreamProvider} using inputFile parameter.
     */
    public PgDumpLoader(Path inputFile, ISettings settings, IProgressMonitor monitor, int monitoringLevel) {
        this(() -> Files.newInputStream(inputFile), inputFile.toString(), settings, monitor, monitoringLevel);
    }

    /**
     * @see #PgDumpLoader(Path, PgDiffArguments, IProgressMonitor, int)
     * @see #PgDumpLoader(InputStreamProvider, String, PgDiffArguments, IProgressMonitor, int)
     */
    public PgDumpLoader(Path inputFile, ISettings settings, IProgressMonitor monitor) {
        this(inputFile, settings, monitor, 1);
    }

    /**
     * @see #PgDumpLoader(Path, PgDiffArguments, IProgressMonitor, int)
     * @see #PgDumpLoader(InputStreamProvider, String, PgDiffArguments, IProgressMonitor, int)
     */
    public PgDumpLoader(Path inputFile, ISettings settings) {
        this(inputFile, settings, new NullProgressMonitor(), 0);
    }

    @Override
    public AbstractDatabase load() throws IOException, InterruptedException {
        AbstractDatabase d = createDb(settings);
        loadAsync(d, antlrTasks);
        finishLoaders();
        return d;
    }

    public AbstractDatabase loadAsync(AbstractDatabase d, Queue<AntlrTask<?>> antlrTasks)
            throws InterruptedException {
        AbstractSchema schema = switch (settings.getDbType()) {
        case MS -> new MsSchema(Consts.DBO);
        case PG -> new PgSchema(Consts.PUBLIC);
        case CH -> new ChSchema(Consts.CH_DEFAULT_DB);
        default -> throw new IllegalArgumentException(Messages.DatabaseType_unsupported_type + settings.getDbType());
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

    public AbstractDatabase loadDatabase(AbstractDatabase intoDb, Queue<AntlrTask<?>> antlrTasks)
            throws InterruptedException {
        PgDiffUtils.checkCancelled(monitor);
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