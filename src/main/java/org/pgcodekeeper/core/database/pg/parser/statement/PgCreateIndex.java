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
package org.pgcodekeeper.core.database.pg.parser.statement;

import java.util.*;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.api.schema.ISchema;
import org.pgcodekeeper.core.database.api.schema.IStatementContainer;
import org.pgcodekeeper.core.database.base.parser.QNameParser;
import org.pgcodekeeper.core.database.pg.parser.generated.SQLParser.*;
import org.pgcodekeeper.core.database.pg.parser.launcher.PgIndexAnalysisLauncher;
import org.pgcodekeeper.core.database.pg.schema.*;
import org.pgcodekeeper.core.settings.ISettings;

/**
 * Parser for PostgreSQL CREATE INDEX statements.
 * <p>
 * This class handles parsing of index definitions including unique indexes,
 * partial indexes, expression indexes, and various index parameters such as
 * storage parameters, tablespace, and included columns.
 */
public final class PgCreateIndex extends PgParserAbstract {

    private final Create_index_statementContext ctx;
    private final String tablespace;
    private final CommonTokenStream stream;

    /**
     * Constructs a new CreateIndex parser.
     *
     * @param ctx        the CREATE INDEX statement context
     * @param db         the PostgreSQL database object
     * @param tablespace the default tablespace name
     * @param stream     the token stream for parsing
     * @param settings   the ISettings object
     */
    public PgCreateIndex(Create_index_statementContext ctx, PgDatabase db, String tablespace, CommonTokenStream stream,
                       ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
        this.tablespace = tablespace;
        this.stream = stream;
    }

    /**
     * Parses index definition from an index rest context and populates the given index object.
     *
     * @param rest       the index rest context containing index definition
     * @param tablespace the default tablespace name
     * @param schemaName the schema name containing the index
     * @param tableName  the table name for the index
     * @param ind        the index object to populate
     * @param db         the PostgreSQL database object
     * @param location   the source location for error reporting
     * @param stream     the token stream for parsing
     * @param settings   the ISettings object
     */
    public static void parseIndex(Index_restContext rest, String tablespace, String schemaName, String tableName,
                                  PgIndex ind, PgDatabase db, String location, CommonTokenStream stream, ISettings settings) {
        new PgCreateIndex(null, db, tablespace, stream, settings).parseIndex(rest, schemaName, tableName, ind, location);
    }

    @Override
    public void parseObject() {
        List<ParserRuleContext> ids = getIdentifiers(ctx.table_name);
        String schemaName = getSchemaNameSafe(ids);
        String tableName = QNameParser.getFirstName(ids);
        addObjReference(ids, DbObjType.TABLE, null);

        IdentifierContext nameCtx = ctx.name;
        String name = nameCtx != null ? nameCtx.getText() : "";
        PgIndex ind = new PgIndex(name);
        parseIndex(ctx.index_rest(), schemaName, tableName, ind, fileName);
        ind.setUnique(ctx.UNIQUE() != null);

        if (nameCtx != null) {
            ParserRuleContext parent = QNameParser.getFirstNameCtx(ids);
            IStatementContainer table = getSafe(ISchema::getStatementContainer, getSchemaSafe(ids), parent);
            addSafe(table, ind, Arrays.asList(QNameParser.getSchemaNameCtx(ids), nameCtx));
        }
    }

    private void parseIndex(Index_restContext rest, String schemaName, String tableName, PgIndex ind, String location) {
        db.addAnalysisLauncher(new PgIndexAnalysisLauncher(ind, rest, location));

        fillSimpleColumns(ind, rest.index_columns().index_column(), null);

        if (rest.method != null) {
            ind.setMethod(rest.method.getText());
        }

        Including_indexContext incl = rest.including_index();
        if (incl != null) {
            fillIncludingDepcy(incl, ind, schemaName, tableName);
            for (IdentifierContext col : incl.identifier()) {
                ind.addInclude(col.getText());
            }
        }

        Nulls_distinctionContext dist = rest.nulls_distinction();
        ind.setNullsDistinction(dist == null || dist.NOT() == null);

        With_storage_parameterContext options = rest.with_storage_parameter();
        if (options != null) {
            for (Storage_parameter_optionContext option : options.storage_parameters().storage_parameter_option()) {
                String key = option.storage_parameter_name().getText();
                VexContext v = option.vex();
                String value = v == null ? "" : v.getText();
                fillOptionParams(value, key, false, ind::addOption);
            }
        }

        if (rest.table_space() != null) {
            ind.setTablespace(getFullCtxText(rest.table_space().identifier()));
        } else if (tablespace != null) {
            ind.setTablespace(tablespace);
        }

        Index_whereContext wherePart = rest.index_where();
        if (wherePart != null) {
            ind.setWhere(getExpressionText(wherePart.vex(), stream));
        }
    }

    @Override
    protected String getStmtAction() {
        StringBuilder sb = new StringBuilder(ACTION_CREATE).append(' ').append(DbObjType.INDEX)
                .append(' ').append(QNameParser.getSchemaName(getIdentifiers(ctx.table_name)));
        if (ctx.name != null) {
            sb.append('.').append(ctx.name.getText());
        }
        return sb.toString();
    }
}
