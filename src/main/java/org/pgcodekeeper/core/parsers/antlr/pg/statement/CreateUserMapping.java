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
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.*;
import org.pgcodekeeper.core.database.pg.schema.PgDatabase;
import org.pgcodekeeper.core.database.pg.schema.PgUserMapping;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.Collections;
import java.util.List;

/**
 * Parser for PostgreSQL CREATE USER MAPPING statements.
 * <p>
 * This class handles parsing of user mapping definitions that associate
 * database users with foreign servers, including user-specific options
 * for authentication and connection parameters. User mappings are used
 * with foreign data wrappers to access external data sources.
 */
public final class CreateUserMapping extends PgParserAbstract {

    private final Create_user_mapping_statementContext ctx;

    /**
     * Constructs a new CreateUserMapping parser.
     *
     * @param ctx      the CREATE USER MAPPING statement context
     * @param db       the PostgreSQL database object
     * @param settings the ISettings object
     */
    public CreateUserMapping(Create_user_mapping_statementContext ctx, PgDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        User_mapping_nameContext userMapping = ctx.user_mapping_name();
        User_nameContext userName = userMapping.user_name();

        if (userName == null) {
            return;
        }
        String server = userMapping.identifier().getText();

        PgUserMapping usm = new PgUserMapping(userName.getText(), server);
        addDepSafe(usm, Collections.singletonList(userMapping.identifier()), DbObjType.SERVER);

        Define_foreign_optionsContext options = ctx.define_foreign_options();
        if (options != null) {
            for (Foreign_optionContext option : options.foreign_option()) {
                fillOptionParams(option.sconst().getText(), option.col_label().getText(), false, usm::addOption);
            }
        }
        addSafe(db, usm, List.of(userMapping));
    }

    @Override
    protected String getStmtAction() {
        return ACTION_CREATE + ' ' + DbObjType.USER_MAPPING + " " +
                getUserMappingName(ctx.user_mapping_name());
    }
}
