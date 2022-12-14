/**
 * Copyright 2006 StartNet s.r.o.
 *
 * Distributed under MIT license
 */
package ru.taximaxim.codekeeper.core.loader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Queue;

import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.core.runtime.NullProgressMonitor;

import ru.taximaxim.codekeeper.core.Consts;
import ru.taximaxim.codekeeper.core.PgDiffArguments;
import ru.taximaxim.codekeeper.core.PgDiffUtils;
import ru.taximaxim.codekeeper.core.fileutils.InputStreamProvider;
import ru.taximaxim.codekeeper.core.model.difftree.DbObjType;
import ru.taximaxim.codekeeper.core.parsers.antlr.AntlrContextProcessor.SqlContextProcessor;
import ru.taximaxim.codekeeper.core.parsers.antlr.AntlrContextProcessor.TSqlContextProcessor;
import ru.taximaxim.codekeeper.core.parsers.antlr.AntlrParser;
import ru.taximaxim.codekeeper.core.parsers.antlr.AntlrTask;
import ru.taximaxim.codekeeper.core.parsers.antlr.CustomSQLParserListener;
import ru.taximaxim.codekeeper.core.parsers.antlr.CustomTSQLParserListener;
import ru.taximaxim.codekeeper.core.parsers.antlr.SQLOverridesListener;
import ru.taximaxim.codekeeper.core.parsers.antlr.TSQLOverridesListener;
import ru.taximaxim.codekeeper.core.schema.AbstractSchema;
import ru.taximaxim.codekeeper.core.schema.GenericColumn;
import ru.taximaxim.codekeeper.core.schema.MsSchema;
import ru.taximaxim.codekeeper.core.schema.PgDatabase;
import ru.taximaxim.codekeeper.core.schema.PgObjLocation;
import ru.taximaxim.codekeeper.core.schema.PgSchema;
import ru.taximaxim.codekeeper.core.schema.PgStatement;
import ru.taximaxim.codekeeper.core.schema.StatementOverride;

/**
 * Loads PostgreSQL dump into classes.
 *
 * @author fordfrog
 */
public class PgDumpLoader extends DatabaseLoader {

    private final InputStreamProvider input;
    private final String inputObjectName;
    private final PgDiffArguments args;

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
            PgDiffArguments args, IProgressMonitor monitor, int monitoringLevel) {
        this.input = input;
        this.inputObjectName = inputObjectName;
        this.args = args;
        this.monitor = monitor;
        this.monitoringLevel = monitoringLevel;
    }

    /**
     * This constructor sets the monitoring level to the default of 1.
     */
    public PgDumpLoader(InputStreamProvider input, String inputObjectName,
            PgDiffArguments args, IProgressMonitor monitor) {
        this(input, inputObjectName, args, monitor, 1);
    }

    /**
     * This constructor uses {@link NullProgressMonitor}.
     */
    public PgDumpLoader(InputStreamProvider input, String inputObjectName, PgDiffArguments args) {
        this(input, inputObjectName, args, new NullProgressMonitor(), 0);
    }

    /**
     * This constructor creates {@link InputStreamProvider} using inputFile parameter.
     */
    public PgDumpLoader(Path inputFile, PgDiffArguments args, IProgressMonitor monitor, int monitoringLevel) {
        this(() -> Files.newInputStream(inputFile), inputFile.toString(), args, monitor, monitoringLevel);
    }

    /**
     * @see #PgDumpLoader(Path, PgDiffArguments, IProgressMonitor, int)
     * @see #PgDumpLoader(InputStreamProvider, String, PgDiffArguments, IProgressMonitor, int)
     */
    public PgDumpLoader(Path inputFile, PgDiffArguments args, IProgressMonitor monitor) {
        this(inputFile, args, monitor, 1);
    }

    /**
     * @see #PgDumpLoader(Path, PgDiffArguments, IProgressMonitor, int)
     * @see #PgDumpLoader(InputStreamProvider, String, PgDiffArguments, IProgressMonitor, int)
     */
    public PgDumpLoader(Path inputFile, PgDiffArguments args) {
        this(inputFile, args, new NullProgressMonitor(), 0);
    }

    @Override
    public PgDatabase load() throws IOException, InterruptedException {
        PgDatabase d = new PgDatabase(args);
        loadAsync(d, antlrTasks);
        finishLoaders();
        return d;
    }

    public PgDatabase loadAsync(PgDatabase d, Queue<AntlrTask<?>> antlrTasks)
            throws InterruptedException {
        AbstractSchema schema = args.isMsSql() ? new MsSchema(Consts.DBO) :
            new PgSchema(Consts.PUBLIC);
        d.addSchema(schema);
        PgObjLocation loc = new PgObjLocation.Builder()
                .setObject(new GenericColumn(schema.getName(), DbObjType.SCHEMA))
                .build();

        schema.setLocation(loc);
        d.setDefaultSchema(schema.getName());
        loadDatabase(d, antlrTasks);
        return d;
    }

    public PgDatabase loadDatabase(PgDatabase intoDb, Queue<AntlrTask<?>> antlrTasks)
            throws InterruptedException {
        PgDiffUtils.checkCancelled(monitor);

        if (args.isMsSql()) {
            TSqlContextProcessor listener;
            if (overrides != null) {
                listener = new TSQLOverridesListener(
                        intoDb, inputObjectName, mode, errors, monitor, overrides);
            } else {
                listener = new CustomTSQLParserListener(
                        intoDb, inputObjectName, mode, errors, monitor);
            }
            AntlrParser.parseTSqlStream(input, args.getInCharsetName(), inputObjectName, errors,
                    monitor, monitoringLevel, listener, antlrTasks);
        } else {
            SqlContextProcessor listener;
            if (overrides != null) {
                listener = new SQLOverridesListener(
                        intoDb, inputObjectName, mode, errors, monitor, overrides);
            } else {
                listener = new CustomSQLParserListener(intoDb,
                        inputObjectName, mode, errors, antlrTasks, monitor);
            }

            AntlrParser.parseSqlStream(input, args.getInCharsetName(), inputObjectName, errors,
                    monitor, monitoringLevel, listener, antlrTasks);
        }

        return intoDb;
    }
}
