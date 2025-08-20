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
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.Alter_domain_statementContext;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.Domain_constraintContext;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.IdentifierContext;
import org.pgcodekeeper.core.schema.pg.PgConstraintCheck;
import org.pgcodekeeper.core.schema.pg.PgDatabase;
import org.pgcodekeeper.core.schema.pg.PgDomain;
import org.pgcodekeeper.core.schema.pg.PgSchema;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.List;

/**
 * Parser for PostgreSQL ALTER DOMAIN statements.
 * <p>
 * This class handles parsing of domain alterations, primarily focused on
 * adding check constraints to existing domains.
 */
public final class AlterDomain extends PgParserAbstract {

    private final Alter_domain_statementContext ctx;

    /**
     * Constructs a new AlterDomain parser.
     *
     * @param ctx      the ALTER DOMAIN statement context
     * @param db       the PostgreSQL database object
     * @param settings the ISettings object
     */
    public AlterDomain(Alter_domain_statementContext ctx, PgDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        List<ParserRuleContext> ids = getIdentifiers(ctx.name);
        PgDomain domain = getSafe(PgSchema::getDomain,
                getSchemaSafe(ids), QNameParser.getFirstNameCtx(ids));

        Domain_constraintContext constrCtx = ctx.dom_constraint;
        if (constrCtx != null && constrCtx.CHECK() != null) {
            IdentifierContext name = constrCtx.name;
            var constrCheck = new PgConstraintCheck(name != null ? name.getText() : "");
            CreateDomain.parseDomainConstraint(domain, constrCheck, constrCtx, db, fileName, settings);
            if (ctx.not_valid != null) {
                constrCheck.setNotValid(true);
            }
            doSafe(PgDomain::addConstraint, domain, constrCheck);
        }

        addObjReference(ids, DbObjType.DOMAIN, ACTION_ALTER);
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_ALTER, DbObjType.DOMAIN, getIdentifiers(ctx.name));
    }
}
