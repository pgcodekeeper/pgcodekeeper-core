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
package org.pgcodekeeper.core.parsers.antlr.pg.statement;

import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.Cast_nameContext;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.Create_cast_statementContext;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.Data_typeContext;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.Schema_qualified_nameContext;
import org.pgcodekeeper.core.database.api.schema.ICast.CastContext;
import org.pgcodekeeper.core.database.pg.schema.PgCast;
import org.pgcodekeeper.core.database.pg.schema.PgCast.CastMethod;
import org.pgcodekeeper.core.database.pg.schema.PgDatabase;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.List;

/**
 * Parser for PostgreSQL CREATE CAST statements.
 * <p>
 * This class handles parsing of type cast definitions including function-based
 * casts, binary-compatible casts, and I/O conversion casts. It processes the
 * source and target types, cast method, and cast context (implicit, assignment, explicit).
 */
public final class CreateCast extends PgParserAbstract {

    private final Create_cast_statementContext ctx;

    /**
     * Constructs a new CreateCast parser.
     *
     * @param ctx      the CREATE CAST statement context
     * @param db       the PostgreSQL database object
     * @param settings the ISettings object
     */
    public CreateCast(Create_cast_statementContext ctx, PgDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        Cast_nameContext nameCtx = ctx.cast_name();
        Data_typeContext source = nameCtx.source;
        Data_typeContext target = nameCtx.target;
        PgCast cast = new PgCast(getFullCtxText(source), getFullCtxText(target));

        addTypeDepcy(source, cast);
        addTypeDepcy(target, cast);

        Schema_qualified_nameContext funcNameCtx = ctx.func_name;
        if (funcNameCtx != null) {
            cast.setMethod(CastMethod.FUNCTION);
            String args = getFullCtxText(ctx.function_args());
            addDepSafe(cast, getIdentifiers(funcNameCtx), DbObjType.FUNCTION, args);
            cast.setFunction(getFullCtxText(funcNameCtx) + args);
        } else if (ctx.INOUT() != null) {
            cast.setMethod(CastMethod.INOUT);
        }

        if (ctx.ASSIGNMENT() != null) {
            cast.setContext(CastContext.ASSIGNMENT);
        } else if (ctx.IMPLICIT() != null) {
            cast.setContext(CastContext.IMPLICIT);
        }

        addSafe(db, cast, List.of(nameCtx));
    }

    @Override
    protected String getStmtAction() {
        return ACTION_CREATE + ' ' + DbObjType.CAST + " (" +
                getCastName(ctx.cast_name()) +
                ')';
    }
}
