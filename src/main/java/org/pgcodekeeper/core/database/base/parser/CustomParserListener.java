/*******************************************************************************
 * Copyright 2017-2026 TAXTELECOM, LLC
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
package org.pgcodekeeper.core.database.base.parser;

import java.util.*;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.pgcodekeeper.core.database.api.parser.ParserListenerMode;
import org.pgcodekeeper.core.database.api.schema.IDatabase;
import org.pgcodekeeper.core.database.api.schema.ObjectLocation;
import org.pgcodekeeper.core.database.base.parser.statement.ParserAbstract;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.exception.*;
import org.pgcodekeeper.core.monitor.IMonitor;
import org.pgcodekeeper.core.settings.DiffSettings;
import org.pgcodekeeper.core.settings.ISettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for custom ANTLR parse tree listeners that build database schema models.
 * Provides common functionality for error handling and statement processing.
 *
 * @param <T> the type of database schema being built
 */
public class CustomParserListener<T extends IDatabase> {

    private static final Logger LOG = LoggerFactory.getLogger(CustomParserListener.class);

    protected final T db;
    protected final ParserListenerMode mode;
    protected final String filename;
    protected final DiffSettings diffSettings;

    /**
     * Creates a new parser listener for building database schemas.
     *
     * @param database     the target database schema
     * @param filename     name of the file being parsed
     * @param mode         parsing mode
     * @param diffSettings unified context object containing settings, monitor, and error accumulator
     */
    public CustomParserListener(T database, String filename,
                                ParserListenerMode mode, DiffSettings diffSettings) {
        this.db = database;
        this.filename = filename;
        this.mode = mode;
        this.diffSettings = diffSettings;
    }

    public ISettings getSettings() {
        return diffSettings.getSettings();
    }

    public IMonitor getMonitor() {
        return diffSettings.getMonitor();
    }

    /**
     * @param ctx statememnt's first token rule
     */
    protected void safeParseStatement(ParserAbstract<T> p, ParserRuleContext ctx) {
        safeParseStatement(() -> p.parseObject(filename, mode, ctx), ctx);
    }

    protected void safeParseStatement(Runnable r, ParserRuleContext ctx) {
        try {
            IMonitor.checkCancelled(getMonitor());
            r.run();
        } catch (UnresolvedReferenceException ex) {
            diffSettings.addError(handleUnresolvedReference(ex, filename));
        } catch (InterruptedException ex) {
            throw new MonitorCancelledRuntimeException();
        } catch (Exception e) {
            if (ctx != null) {
                diffSettings.addError(handleParserContextException(e, filename, ctx));
            } else {
                LOG.error("Statement context is missing", e);
            }
        }
    }

    /**
     * Handles unresolved reference exceptions during parsing.
     *
     * @param ex       the unresolved reference exception
     * @param filename name of the file being parsed
     * @return error object containing details about the failure
     */
    public static AntlrError handleUnresolvedReference(UnresolvedReferenceException ex,
                                                       String filename) {
        Token t = ex.getErrorToken();
        ErrorTypes errorType = ex instanceof MisplacedObjectException ? ErrorTypes.MISPLACEERROR : ErrorTypes.OTHER;
        AntlrError err = new AntlrError(t, filename, t.getLine(),
                ((CodeUnitToken) t).getCodeUnitPositionInLine(), ex.getMessage(), errorType);
        String errorMessage = err.toString();
        LOG.warn(errorMessage, ex);
        return err;
    }

    private AntlrError handleParserContextException(Exception ex, String filename, ParserRuleContext ctx) {
        Token t = ctx.getStart();
        AntlrError err = new AntlrError(t, filename, t.getLine(), ((CodeUnitToken) t).getCodeUnitPositionInLine(),
                ex.getMessage());
        String errorMessage = err.toString();
        if (ex instanceof ObjectCreationException) {
            LOG.warn(errorMessage, ex);
        } else {
            LOG.error(errorMessage, ex);
        }
        return err;
    }

    /**
     * Adding undescribed DB object to query storage for showing information about it in 'Outline' and in 'Console'.
     * <br>
     * <br>
     * 'undescribed DB object' - means that it is not a child of
     * {@link AbstractStatement}.
     */
    protected void addToQueries(ParserRuleContext ctx, String acton) {
        ObjectLocation loc = new ObjectLocation.Builder()
                .setAction(acton)
                .setCtx(ctx)
                .setSql(ParserListenerMode.SCRIPT == mode ? ParserAbstract.getFullCtxText(ctx) : null)
                .build();

        safeParseStatement(() -> db.addReference(filename, loc), ctx);
    }

    /**
     * Returns only the first 'descrWordsCount' words from a query in 'ctx'.
     */
    protected String getActionDescription(ParserRuleContext ctx, int descrWordsCount) {
        StringBuilder descr = new StringBuilder();
        fillActionDescription(ctx, new int[]{descrWordsCount}, descr);
        return descr.toString();
    }

    private void fillActionDescription(ParserRuleContext ctx, int[] descrWordsCount,
                                       StringBuilder descr) {
        List<ParseTree> children = ctx.children;
        if (children == null) {
            return;
        }

        for (ParseTree child : children) {
            if (0 >= descrWordsCount[0]) {
                return;
            }

            if (child instanceof ParserRuleContext ruleCtx) {
                fillActionDescription(ruleCtx, descrWordsCount, descr);
            } else if (child instanceof TerminalNode) {
                descr.append(child.getText().toUpperCase(Locale.ROOT));
                if (0 < --descrWordsCount[0]) {
                    descr.append(' ');
                }
            }
        }
    }
}
