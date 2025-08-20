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

import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.base.QNameParser;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.Collation_optionContext;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.Create_collation_statementContext;
import org.pgcodekeeper.core.schema.pg.PgCollation;
import org.pgcodekeeper.core.schema.pg.PgDatabase;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.List;

/**
 * Parser for PostgreSQL CREATE COLLATION statements.
 * <p>
 * This class handles parsing of collation definitions including locale settings
 * (LC_COLLATE, LC_CTYPE), provider information, rules, and deterministic
 * behavior options for text sorting and comparison.
 */
public final class CreateCollation extends PgParserAbstract {

    private final Create_collation_statementContext ctx;

    /**
     * Constructs a new CreateCollation parser.
     *
     * @param ctx      the CREATE COLLATION statement context
     * @param db       the PostgreSQL database object
     * @param settings the ISettings object
     */
    public CreateCollation(Create_collation_statementContext ctx, PgDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        List<ParserRuleContext> ids = getIdentifiers(ctx.name);
        PgCollation collation = new PgCollation(QNameParser.getFirstName(ids));

        for (Collation_optionContext body : ctx.collation_option()) {
            if (body.LOCALE() != null) {
                collation.setLcCollate(getValue(body));
                collation.setLcCtype(getValue(body));
            } else if (body.LC_COLLATE() != null) {
                collation.setLcCollate(getValue(body));
            } else if (body.LC_CTYPE() != null) {
                collation.setLcCtype(getValue(body));
            } else if (body.PROVIDER() != null) {
                collation.setProvider(getValue(body));
            } else if (body.RULES() != null) {
                collation.setRules(getValue(body));
            } else if (body.DETERMINISTIC() != null) {
                collation.setDeterministic(parseBoolean(body.boolean_value()));
            }
        }
        addSafe(getSchemaSafe(ids), collation, ids);
    }

    private String getValue(Collation_optionContext body) {
        if (body.DEFAULT() != null) {
            return "default";
        }
        var val = body.sconst();
        if (val != null) {
            return val.getText();
        }
        return body.identifier().getText();
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_CREATE, DbObjType.VIEW, ctx.name);
    }
}
