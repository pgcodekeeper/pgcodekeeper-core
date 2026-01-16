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

import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.pg.parser.generated.SQLParser.*;
import org.pgcodekeeper.core.database.pg.schema.*;
import org.pgcodekeeper.core.settings.ISettings;

/**
 * Parser for PostgreSQL CREATE SERVER statements.
 * <p>
 * This class handles parsing of foreign server definitions including
 * server type, version, associated foreign data wrapper, and server-specific
 * options. Foreign servers define connections to external data sources.
 */
public final class PgCreateServer extends PgParserAbstract {

    private final Create_server_statementContext ctx;

    /**
     * Constructs a new CreateServer parser.
     *
     * @param ctx      the CREATE SERVER statement context
     * @param db       the PostgreSQL database object
     * @param settings the ISettings object
     */
    public PgCreateServer(Create_server_statementContext ctx, PgDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        List<IdentifierContext> ids = ctx.identifier();
        String name = ids.get(0).getText();
        PgServer server = new PgServer(name);
        if (ctx.type != null) {
            server.setType(getFullCtxText(ctx.type));
        }
        if (ctx.version != null) {
            server.setVersion(getFullCtxText(ctx.version));
        }
        server.setFdw(ids.get(1).getText());
        addDepSafe(server, Collections.singletonList(ids.get(1)), DbObjType.FOREIGN_DATA_WRAPPER);

        Define_foreign_optionsContext options = ctx.define_foreign_options();
        if (options != null) {
            for (Foreign_optionContext option : options.foreign_option()) {
                fillOptionParams(option.sconst().getText(), option.col_label().getText(), false, server::addOption);
            }
        }
        addSafe(db, server, Collections.singletonList(ids.get(0)));
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_CREATE, DbObjType.SERVER, ctx.identifier().get(0));
    }
}
