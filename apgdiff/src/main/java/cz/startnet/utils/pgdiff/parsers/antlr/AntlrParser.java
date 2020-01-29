package cz.startnet.utils.pgdiff.parsers.antlr;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.core.runtime.NullProgressMonitor;

import cz.startnet.utils.pgdiff.PgDiffUtils;
import cz.startnet.utils.pgdiff.parsers.antlr.AntlrContextProcessor.SqlContextProcessor;
import cz.startnet.utils.pgdiff.parsers.antlr.AntlrContextProcessor.TSqlContextProcessor;
import cz.startnet.utils.pgdiff.parsers.antlr.exception.MonitorCancelledRuntimeException;
import cz.startnet.utils.pgdiff.parsers.antlr.exception.UnresolvedReferenceException;
import ru.taximaxim.codekeeper.apgdiff.DaemonThreadFactory;
import ru.taximaxim.codekeeper.apgdiff.fileutils.InputStreamProvider;
import ru.taximaxim.codekeeper.apgdiff.log.Log;
import ru.taximaxim.codekeeper.apgdiff.utils.Pair;

public class AntlrParser {

    private static final String POOL_SIZE = "ru.taximaxim.codekeeper.parser.poolsize";

    private static final ExecutorService ANTLR_POOL;

    private static volatile long pgParserLastStart;
    private static volatile long msParserLastStart;

    static {
        int count = Integer.getInteger(
                POOL_SIZE, Runtime.getRuntime().availableProcessors() - 1);
        ANTLR_POOL = Executors.newFixedThreadPool(
                Integer.max(1, count), new DaemonThreadFactory());
    }

    /**
     * Constructs a <code>parserClass</code> {@link Parser} object with the stream as the token source
     * and {@link CustomAntlrErrorListener} as parser and lexer error listener.
     */
    public static <T extends Parser> T makeBasicParser(Class<T> parserClass,
            InputStream stream, String charset, String parsedObjectName) throws IOException {
        return makeBasicParser(
                parserClass, new ANTLRInputStream(new InputStreamReader(stream, charset)),
                parsedObjectName, null);
    }

    public static <T extends Parser> T makeBasicParser(Class<T> parserClass,
            InputStream stream, String charset, String parsedObjectName,
            List<Object> errors) throws IOException {
        return makeBasicParser(
                parserClass, new ANTLRInputStream(new InputStreamReader(stream, charset)),
                parsedObjectName, errors);
    }

    /**
     * Constructs a <code>parserClass</code> {@link Parser} object with the string as the token source
     * and {@link CustomAntlrErrorListener} as parser and lexer error listener.
     */
    public static <T extends Parser> T makeBasicParser(Class<T> parserClass, String string,
            String parsedObjectName) {
        return makeBasicParser(parserClass, new ANTLRInputStream(string), parsedObjectName, null);
    }

    public static <T extends Parser> T makeBasicParser(Class<T> parserClass, String string,
            String parsedObjectName, List<Object> errors) {
        return makeBasicParser(parserClass, new ANTLRInputStream(string), parsedObjectName, errors);
    }

    private static <T extends Parser> T makeBasicParser(Class<T> parserClass,
            ANTLRInputStream stream, String parsedObjectName, List<Object> errors) {
        return makeBasicParser(parserClass, stream, parsedObjectName, errors, 0, 0, 0);
    }

    /*
     * Because INTO is sometimes used in the main SQL grammar, we have to be
     * careful not to take any such usage of INTO as a PL/pgSQL INTO clause.
     * There are currently three such cases:
     *
     * 1. SELECT ... INTO.  We don't care, we just override that with the
     * PL/pgSQL definition.
     *
     * 2. INSERT INTO.  This is relatively easy to recognize since the words
     * must appear adjacently; but we can't assume INSERT starts the command,
     * because it can appear in CREATE RULE or WITH.  Unfortunately, INSERT is
     * *not* fully reserved, so that means there is a chance of a false match;
     * but it's not very likely.
     *
     * 3. IMPORT FOREIGN SCHEMA ... INTO.  This is not allowed in CREATE RULE
     * or WITH, so we just check for IMPORT as the command's first token.
     * (If IMPORT FOREIGN SCHEMA returned data someone might wish to capture
     * with an INTO-variables clause, we'd have to work much harder here.)
     */
    public static <T extends Parser> void removeIntoStatements(T parser) {
        CommonTokenStream stream = (CommonTokenStream) parser.getTokenStream();

        boolean isImport = false;
        boolean hasInto = false;
        int i = 0;

        while (true) {
            stream.seek(i++);
            int type = stream.LA(1);

            switch (type) {
            case SQLLexer.EOF:
                stream.reset();
                parser.setInputStream(stream);
                return;
            case SQLLexer.SEMI_COLON:
                isImport = false;
                hasInto = false;
                break;
            case SQLLexer.IMPORT:
                if (stream.LA(2) == SQLLexer.FOREIGN && stream.LA(3) == SQLLexer.SCHEMA) {
                    isImport = true;
                }
                break;
            case SQLLexer.INTO:
                if (hasInto || isImport || stream.LA(- 1) == SQLLexer.INSERT) {
                    break;
                }
                if (hideIntoTokens(stream)) {
                    hasInto = true;
                    i--; // back to previous index, because we hide current token
                }
                break;
            default:
                break;
            }
        }
    }

    private static boolean hideIntoTokens(CommonTokenStream stream) {
        int i = 1;
        List<CommonToken> tokens = new ArrayList<>();
        tokens.add((CommonToken) stream.LT(i++));

        int nextType = stream.LA(i);

        if (nextType == SQLLexer.STRICT) {
            tokens.add((CommonToken) stream.LT(i++));
            nextType = stream.LA(i);
        }

        if (isIdentifier(nextType)) {
            tokens.add((CommonToken) stream.LT(i++));
            nextType = stream.LA(i);

            while ((nextType == SQLLexer.DOT || nextType == SQLLexer.COMMA)
                    && isIdentifier(stream.LA(i + 1))) {
                // comma or dot
                tokens.add((CommonToken) stream.LT(i++));
                // identifier
                tokens.add((CommonToken) stream.LT(i++));

                nextType = stream.LA(i);
            }

            tokens.forEach(t -> t.setChannel(Token.HIDDEN_CHANNEL));
            return true;
        }

        return false;
    }

    private static boolean isIdentifier(int type) {
        return SQLLexer.ABORT <= type || type <= SQLLexer.WHILE
                || type == SQLLexer.Identifier || type == SQLLexer.QuotedIdentifier;
    }

    public static <T extends Parser> T makeBasicParser(Class<T> parserClass, String string,
            String parsedObjectName, List<Object> errors, TerminalNode codeStart) {
        int offset = codeStart.getSymbol().getStartIndex();
        int lineOffset = codeStart.getSymbol().getLine() - 1;
        int inLineOffset = codeStart.getSymbol().getCharPositionInLine();
        return makeBasicParser(parserClass, new ANTLRInputStream(string),
                parsedObjectName, errors, offset, lineOffset, inLineOffset);
    }

    private static <T extends Parser> T makeBasicParser(Class<T> parserClass,
            ANTLRInputStream stream, String parsedObjectName, List<Object> errors,
            int offset, int lineOffset, int inLineOffset) {
        Lexer lexer;
        Parser parser;
        if (parserClass.isAssignableFrom(SQLParser.class)) {
            lexer = new SQLLexer(stream);
            parser = new SQLParser(new CommonTokenStream(lexer));
            parser.setErrorHandler(new CustomSQLAntlrErrorStrategy());
        } else if (parserClass.isAssignableFrom(TSQLParser.class)) {
            lexer = new TSQLLexer(stream);
            parser = new TSQLParser(new CommonTokenStream(lexer));
            parser.setErrorHandler(new CustomTSQLAntlrErrorStrategy());
        } else if (parserClass.isAssignableFrom(IgnoreListParser.class)) {
            lexer = new IgnoreListLexer(stream);
            parser = new IgnoreListParser(new CommonTokenStream(lexer));
        } else if (parserClass.isAssignableFrom(PrivilegesParser.class)) {
            lexer = new PrivilegesLexer(stream);
            parser = new PrivilegesParser(new CommonTokenStream(lexer));
        } else {
            throw new IllegalArgumentException("Unknown parser class: " + parserClass);
        }

        CustomAntlrErrorListener err = new CustomAntlrErrorListener(
                parsedObjectName, errors, offset, lineOffset, inLineOffset);
        lexer.removeErrorListeners();
        lexer.addErrorListener(err);
        parser.removeErrorListeners();
        parser.addErrorListener(err);

        return parserClass.cast(parser);
    }

    public static void parseSqlStream(InputStreamProvider inputStream, String charsetName,
            String parsedObjectName, List<Object> errors, IProgressMonitor mon, int monitoringLevel,
            SqlContextProcessor listener, Queue<AntlrTask<?>> antlrTasks)
                    throws InterruptedException {
        submitAntlrTask(antlrTasks, () -> {
            PgDiffUtils.checkCancelled(mon);
            try(InputStream stream = inputStream.getStream()) {
                SQLParser parser = makeBasicParser(SQLParser.class, stream,
                        charsetName, parsedObjectName, errors);
                parser.addParseListener(new CustomParseTreeListener(
                        monitoringLevel, mon == null ? new NullProgressMonitor() : mon));
                saveTimeOfLastParserStart(false);
                return new Pair<>(parser.sql(), (CommonTokenStream) parser.getTokenStream());
            } catch (MonitorCancelledRuntimeException mcre){
                throw new InterruptedException();
            }
        }, pair -> {
            try {
                listener.process(pair.getFirst(), pair.getSecond());
            } catch (UnresolvedReferenceException ex) {
                errors.add(CustomSQLParserListener.handleUnresolvedReference(ex, parsedObjectName));
            }
        });
    }

    public static void parseTSqlStream(InputStreamProvider inputStream, String charsetName,
            String parsedObjectName, List<Object> errors, IProgressMonitor mon, int monitoringLevel,
            TSqlContextProcessor listener, Queue<AntlrTask<?>> antlrTasks)
                    throws InterruptedException {
        submitAntlrTask(antlrTasks, () -> {
            PgDiffUtils.checkCancelled(mon);
            try(InputStream stream = inputStream.getStream()) {
                TSQLParser parser = makeBasicParser(TSQLParser.class,
                        stream, charsetName, parsedObjectName, errors);
                parser.addParseListener(new CustomParseTreeListener(
                        monitoringLevel, mon == null ? new NullProgressMonitor() : mon));
                saveTimeOfLastParserStart(true);
                return new Pair<>(parser, parser.tsql_file());
            } catch (MonitorCancelledRuntimeException mcre){
                throw new InterruptedException();
            }
        }, pair -> {
            try {
                listener.process(pair.getSecond(),
                        (CommonTokenStream) pair.getFirst().getInputStream());
            } catch (UnresolvedReferenceException ex) {
                errors.add(CustomTSQLParserListener.handleUnresolvedReference(ex, parsedObjectName));
            }
        });
    }

    public static <T extends ParserRuleContext, P extends Parser>
    T parseSqlString(Class<P> parserClass, Function<P, T> parserEntry, String sql,
            String parsedObjectName, List<Object> errors) {
        Future<T> f = submitAntlrTask(() -> parserEntry.apply(
                makeBasicParser(parserClass, sql, parsedObjectName, errors)));
        try {
            saveTimeOfLastParserStart(parserClass.isAssignableFrom(TSQLParser.class));
            return f.get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(ex);
        } catch (ExecutionException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public static <T> Future<T> submitAntlrTask(Callable<T> task) {
        return ANTLR_POOL.submit(task);
    }

    public static <T> void submitAntlrTask(Queue<AntlrTask<?>> antlrTasks,
            Callable<T> task, Consumer<T> finalizer) {
        Future<T> future = submitAntlrTask(task);
        antlrTasks.add(new AntlrTask<>(future, finalizer));
    }

    public static void finishAntlr(Queue<AntlrTask<?>> antlrTasks)
            throws InterruptedException, IOException {
        AntlrTask<?> task;
        try {
            while ((task = antlrTasks.poll()) != null) {
                task.finish();
            }
        } catch (ExecutionException ex) {
            handleAntlrTaskException(ex);
        } catch (MonitorCancelledRuntimeException ex) {
            // finalizing parser listeners' cancellations will reach here
            throw new InterruptedException();
        }
    }

    /**
     * Uwraps potential parser Interrupted and IO Exceptions from ExecutionException.<br>
     * If non-standard parser exception is caught in the wrapper, it is rethrown
     * as an IllegalStateException.
     *
     * @throws InterruptedException
     * @throws IOException
     * @throws IllegalStateException
     */
    public static void handleAntlrTaskException(ExecutionException ex)
            throws InterruptedException, IOException {
        Throwable t = ex.getCause();
        if (t instanceof InterruptedException) {
            throw (InterruptedException) t;
        } else if (t instanceof IOException) {
            throw (IOException) t;
        } else {
            throw new IllegalStateException(ex);
        }
    }

    public static void checkToClean(boolean isMsParser, long cleaningInterval) {
        long lastParserStart = isMsParser ? msParserLastStart : pgParserLastStart;
        if (lastParserStart != 0
                && (cleaningInterval < System.currentTimeMillis() - lastParserStart)) {
            cleanParserCache(isMsParser);
        }
    }

    private static void cleanParserCache(boolean isMsParser) {
        Class<? extends Parser> parserClazz = null;
        if (isMsParser) {
            msParserLastStart = 0;
            parserClazz = TSQLParser.class;
        } else {
            pgParserLastStart = 0;
            parserClazz = SQLParser.class;
        }
        makeBasicParser(parserClazz, ";", "fake string to clean parser cache")
        .getInterpreter().clearDFA();
    }

    public static void cleanCacheOfBothParsers() {
        if (pgParserLastStart != 0) {
            cleanParserCache(false);
        }
        if (msParserLastStart != 0) {
            cleanParserCache(true);
        }
    }

    private static void saveTimeOfLastParserStart(boolean isMsParser) {
        if (isMsParser) {
            msParserLastStart = System.currentTimeMillis();
            return;
        }
        pgParserLastStart = System.currentTimeMillis();
    }

    private AntlrParser() {
        // only static
    }
}

class CustomParseTreeListener implements ParseTreeListener{
    private final int monitoringLevel;
    private final IProgressMonitor monitor;

    public CustomParseTreeListener(int monitoringLevel, IProgressMonitor monitor){
        this.monitoringLevel = monitoringLevel;
        this.monitor = monitor;
    }

    @Override
    public void visitTerminal(TerminalNode node) {
        //no imp
    }

    @Override
    public void visitErrorNode(ErrorNode node) {
        //no imp
    }

    @Override
    public void exitEveryRule(ParserRuleContext ctx) {
        if (ctx.depth() <= monitoringLevel) {
            monitor.worked(1);
            try {
                PgDiffUtils.checkCancelled(monitor);
            } catch (InterruptedException e) {
                throw new MonitorCancelledRuntimeException();
            }
        }
    }

    @Override
    public void enterEveryRule(ParserRuleContext ctx) {
        //no imp
    }
}

class CustomAntlrErrorListener extends BaseErrorListener {

    private final String parsedObjectName;
    private final List<Object> errors;
    private final int offset;
    private final int lineOffset;
    private final int inLineOffset;

    public CustomAntlrErrorListener(String parsedObjectName, List<Object> errors,
            int offset, int lineOffset, int inLineOffset) {
        this.parsedObjectName = parsedObjectName;
        this.errors = errors;
        this.offset = offset;
        this.lineOffset = lineOffset;
        this.inLineOffset = inLineOffset;
    }

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
            int line, int charPositionInLine, String msg, RecognitionException e) {
        Token token = offendingSymbol instanceof Token ? (Token) offendingSymbol : null;
        AntlrError error = new AntlrError(token, parsedObjectName, line, charPositionInLine, msg)
                .copyWithOffset(offset, lineOffset, inLineOffset);

        Log.log(Log.LOG_WARNING, "ANTLR Error:\n" + error.toString());
        if (errors != null) {
            errors.add(error);
        }
    }
}
