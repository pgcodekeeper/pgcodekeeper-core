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

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.ms.generated.TSQLParser.*;
import org.pgcodekeeper.core.database.base.schema.AbstractSchema;
import org.pgcodekeeper.core.database.ms.schema.MsDatabase;
import org.pgcodekeeper.core.database.ms.schema.MsSequence;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Parser for Microsoft SQL CREATE SEQUENCE statements.
 * Handles sequence creation with various options including data type, start value,
 * increment, min/max values, cache settings, and cycle options.
 */
public final class CreateMsSequence extends MsParserAbstract {

    private final Create_sequenceContext ctx;

    /**
     * Creates a parser for Microsoft SQL CREATE SEQUENCE statements.
     *
     * @param ctx      the ANTLR parse tree context for the CREATE SEQUENCE statement
     * @param db       the Microsoft SQL database schema being processed
     * @param settings parsing configuration settings
     */
    public CreateMsSequence(Create_sequenceContext ctx, MsDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        IdContext nameCtx = ctx.qualified_name().name;
        String name = nameCtx.getText();
        MsSequence sequence = new MsSequence(name);
        fillSequence(sequence, ctx.sequence_body());
        List<ParserRuleContext> ids = Arrays.asList(ctx.qualified_name().schema, nameCtx);
        AbstractSchema schema = getSchemaSafe(ids);
        addSafe(schema, sequence, ids);
    }

    private void fillSequence(MsSequence sequence, List<Sequence_bodyContext> list) {
        long inc = 1;
        Long maxValue = null;
        Long minValue = null;
        String dataType = null;
        String precision = null;
        for (Sequence_bodyContext body : list) {
            if (body.data_type() != null) {
                Data_typeContext data = body.data_type();
                addTypeDepcy(data, sequence);
                // try to catch tinyint, smallint, int, bigint, numeric, decimal
                dataType = data.qualified_name().getText().toLowerCase(Locale.ROOT);
                sequence.setDataType(getFullCtxText(data));
                Data_type_sizeContext size = data.size;
                if (size != null && size.presicion != null) {
                    precision = size.presicion.getText();
                }
            } else if (body.start_val != null) {
                sequence.setStartWith(body.start_val.getText());
            } else if (body.CACHE() != null && body.NO() == null) {
                sequence.setCached(true);
                if (body.cache_val != null) {
                    sequence.setCache(body.cache_val.getText());
                }
            } else if (body.incr != null) {
                inc = Long.parseLong(body.incr.getText());
            } else if (body.maxval != null) {
                maxValue = Long.parseLong(body.maxval.getText());
            } else if (body.minval != null) {
                minValue = Long.parseLong(body.minval.getText());
            } else if (body.cycle_val != null) {
                sequence.setCycle(body.cycle_true == null);
            }
        }

        sequence.setMinMaxInc(inc, maxValue, minValue, dataType,
                precision == null ? 0L : Long.parseLong(precision));
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_CREATE, DbObjType.SEQUENCE, ctx.qualified_name());
    }
}
