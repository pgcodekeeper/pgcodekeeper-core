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

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.Utils;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.base.QNameParser;
import org.pgcodekeeper.core.parsers.antlr.pg.launcher.DomainAnalysisLauncher;
import org.pgcodekeeper.core.parsers.antlr.pg.launcher.VexAnalysisLauncher;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.*;
import org.pgcodekeeper.core.schema.AbstractDatabase;
import org.pgcodekeeper.core.schema.pg.PgConstraintCheck;
import org.pgcodekeeper.core.schema.pg.PgDatabase;
import org.pgcodekeeper.core.schema.pg.PgDomain;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.List;

/**
 * Parser for PostgreSQL CREATE DOMAIN statements.
 * <p>
 * This class handles parsing of domain definitions including the underlying
 * data type, default values, check constraints, collation, and null constraints.
 * Domains are user-defined data types based on existing types with additional
 * constraints and default values.
 */
public final class CreateDomain extends PgParserAbstract {

    private final Create_domain_statementContext ctx;
    private final CommonTokenStream stream;

    /**
     * Constructs a new CreateDomain parser.
     *
     * @param ctx      the CREATE DOMAIN statement context
     * @param db       the PostgreSQL database object
     * @param stream   the token stream for parsing
     * @param settings the ISettings object
     */
    public CreateDomain(Create_domain_statementContext ctx, PgDatabase db, CommonTokenStream stream, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
        this.stream = stream;
    }

    @Override
    public void parseObject() {
        List<ParserRuleContext> ids = getIdentifiers(ctx.name);
        PgDomain domain = new PgDomain(QNameParser.getFirstName(ids));
        domain.setDataType(getTypeName(ctx.dat_type));
        addTypeDepcy(ctx.dat_type, domain);
        for (Collate_identifierContext coll : ctx.collate_identifier()) {
            domain.setCollation(getFullCtxText(coll.collation));
            addDepSafe(domain, getIdentifiers(coll.collation), DbObjType.COLLATION);
        }
        VexContext exp = ctx.def_value;
        if (exp != null) {
            db.addAnalysisLauncher(new VexAnalysisLauncher(domain, exp, fileName));
            domain.setDefaultValue(getExpressionText(exp, stream));
        }
        for (Domain_constraintContext constrCtx : ctx.dom_constraint) {
            if (constrCtx.CHECK() != null) {
                IdentifierContext name = constrCtx.name;
                var constrCheck = new PgConstraintCheck(name != null ? name.getText() : "");
                parseDomainConstraint(domain, constrCheck, constrCtx, db, fileName, settings);
                domain.addConstraint(constrCheck);
            }
            // вынесено ограничение, т.к. мы привязываем ограничение на нул к
            // объекту а не создаем отдельный констрайнт
            if (constrCtx.NULL() != null) {
                domain.setNotNull(constrCtx.NOT() != null);
            }
        }

        addSafe(getSchemaSafe(ids), domain, ids);
    }

    /**
     * Parses a domain constraint definition and configures the constraint object.
     * <p>
     * This method processes CHECK constraints for domains, including the constraint
     * expression and sets up analysis launchers for dependency tracking.
     *
     * @param domain   the domain object that owns the constraint
     * @param constr   the constraint object to configure
     * @param ctx      the domain constraint context
     * @param db       the database for analysis launchers
     * @param location the source location for error reporting
     * @param settings the parser settings
     */
    public static void parseDomainConstraint(PgDomain domain, PgConstraintCheck constr,
                                             Domain_constraintContext ctx, AbstractDatabase db, String location, ISettings settings) {
        VexContext vexCtx = ctx.vex();
        constr.setExpression(Utils.checkNewLines(getFullCtxText(vexCtx), settings.isKeepNewlines()));
        db.addAnalysisLauncher(new DomainAnalysisLauncher(domain, vexCtx, location));
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_CREATE, DbObjType.DOMAIN, getIdentifiers(ctx.name));
    }
}
