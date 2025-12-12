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
package org.pgcodekeeper.core.parsers.antlr.ms.statement;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;
import org.pgcodekeeper.core.parsers.antlr.base.CodeUnitToken;
import org.pgcodekeeper.core.database.api.schema.ObjectLocation;
import org.pgcodekeeper.core.database.ms.schema.MsSourceStatement;
import org.pgcodekeeper.core.database.ms.schema.MsDatabase;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.List;
import java.util.Locale;

/**
 * Abstract base class for Microsoft SQL parsers that handle batch context processing.
 * Provides functionality for processing statements with source code preservation
 * and proper location tracking for batch-oriented SQL objects like functions, procedures, and views.
 */
public abstract class BatchContextProcessor extends MsParserAbstract {

    private final ParserRuleContext batchCtx;
    private final CommonTokenStream stream;

    protected BatchContextProcessor(MsDatabase db, ParserRuleContext batchCtx,
                                    CommonTokenStream stream, ISettings settings) {
        super(db, settings);
        this.batchCtx = batchCtx;
        this.stream = stream;
    }

    /**
     * @return the context, after which the second source part starts
     */
    protected abstract ParserRuleContext getDelimiterCtx();

    protected void setSourceParts(MsSourceStatement st) {
        String first = getHiddenLeftCtxText(batchCtx, stream);
        st.setFirstPart(checkNewLines(first));

        List<Token> endTokens = stream.getHiddenTokensToRight(batchCtx.getStop().getTokenIndex());
        Token stopToken = endTokens != null ? endTokens.get(endTokens.size() - 1) : batchCtx.getStop();
        int stop = ((CodeUnitToken) stopToken).getCodeUnitStop();
        int start = ((CodeUnitToken) getDelimiterCtx().getStop()).getCodeUnitStop() + 1;
        String second = stopToken.getInputStream().getText(Interval.of(start, stop));
        st.setSecondPart(checkNewLines(second));
    }

    @Override
    protected ObjectLocation fillQueryLocation(ParserRuleContext ctx) {
        String act = getStmtAction();
        List<Token> startTokens = stream.getHiddenTokensToLeft(ctx.getStart().getTokenIndex());
        List<Token> stopTokens = stream.getHiddenTokensToRight(ctx.getStop().getTokenIndex());
        Token start = startTokens != null ? startTokens.get(0) : ctx.getStart();
        Token stop = stopTokens != null ? stopTokens.get(stopTokens.size() - 1) : ctx.getStop();
        String sql = getFullCtxText(start, stop);
        String action = act != null ? act : ctx.getStart().getText().toUpperCase(Locale.ROOT);
        CodeUnitToken cuStart = (CodeUnitToken) start;
        int offset = cuStart.getCodeUnitStart();
        int lineNumber = cuStart.getLine();
        int charPositionInLine = cuStart.getCodeUnitPositionInLine();

        ObjectLocation loc = new ObjectLocation.Builder()
                .setAction(action)
                .setOffset(offset)
                .setLineNumber(lineNumber)
                .setCharPositionInLine(charPositionInLine)
                .setSql(sql)
                .build();

        db.addReference(fileName, loc);
        return loc;
    }
}
