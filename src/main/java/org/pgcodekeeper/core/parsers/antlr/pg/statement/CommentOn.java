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
import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.pg.schema.*;
import org.pgcodekeeper.core.parsers.antlr.base.QNameParser;
import org.pgcodekeeper.core.exception.UnresolvedReferenceException;
import org.pgcodekeeper.core.parsers.antlr.pg.generated.SQLParser.*;
import org.pgcodekeeper.core.database.base.schema.AbstractTable;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.database.base.schema.AbstractStatementContainer;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/**
 * Parser for PostgreSQL COMMENT ON statements.
 * <p>
 * This class handles parsing of comments on various database objects including
 * tables, columns, functions, procedures, operators, constraints, indexes,
 * views, types, domains, sequences, triggers, rules, policies, and other
 * PostgreSQL database objects.
 */
public final class CommentOn extends PgParserAbstract {

    private final Comment_on_statementContext ctx;

    /**
     * Constructs a new CommentOn parser.
     *
     * @param ctx      the COMMENT ON statement context
     * @param db       the PostgreSQL database object
     * @param settings the ISettings object
     */
    public CommentOn(Comment_on_statementContext ctx, PgDatabase db, ISettings settings) {
        super(db, settings);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        SconstContext str = ctx.sconst();
        String comment = str == null ? null : getFullCtxTextWithCheckNewLines(str);
        Comment_member_objectContext obj = ctx.comment_member_object();

        List<ParserRuleContext> ids;
        if (obj.target_operator() != null) {
            ids = getIdentifiers(obj.target_operator().name);
        } else if (obj.name != null) {
            ids = getIdentifiers(obj.name);
        } else if (obj.cast_name() != null) {
            ids = Collections.singletonList(obj.cast_name());
        } else {
            ids = Collections.singletonList(obj.identifier());
        }

        ParserRuleContext nameCtx = QNameParser.getFirstNameCtx(ids);
        String name = nameCtx.getText();

        // column (separately because of non-real columns)
        if (obj.COLUMN() != null) {
            addObjReference(ids, DbObjType.COLUMN, ACTION_COMMENT);

            if (isRefMode()) {
                return;
            }

            List<? extends ParserRuleContext> tableIds = ids.subList(0, 2);
            PgSchema schema = getSchemaSafe(tableIds);

            ParserRuleContext tableCtx = QNameParser.getSecondNameCtx(ids);
            if (tableCtx == null) {
                throw new UnresolvedReferenceException(
                        "Table name is missing for commented column!", nameCtx.getStart());
            }
            String tableName = tableCtx.getText();
            PgAbstractTable table = (PgAbstractTable) schema.getTable(tableName);
            if (table == null) {
                PgAbstractView view = (PgAbstractView) schema.getView(tableName);
                if (view == null) {
                    PgCompositeType t = ((PgCompositeType) getSafe(PgSchema::getType, schema, tableCtx));
                    addObjReference(tableIds, DbObjType.TYPE, null);
                    t.getAttr(name).setComment(comment);
                } else {
                    addObjReference(tableIds, DbObjType.VIEW, null);
                    view.addColumnComment(name, comment);
                }
            } else {
                PgColumn column;
                if (table.getInherits().isEmpty()) {
                    column = (PgColumn) getSafe(AbstractTable::getColumn, table, nameCtx);
                } else {
                    String colName = nameCtx.getText();
                    column = (PgColumn) table.getColumn(colName);
                    if (column == null) {
                        column = new PgColumn(colName);
                        column.setInherit(true);
                        table.addColumn(column);
                    }
                }
                addObjReference(tableIds, DbObjType.TABLE, null);
                column.setComment(comment);
            }
            return;
        }

        PgSchema schema = null;
        if (obj.table_name != null) {
            schema = getSchemaSafe(getIdentifiers(obj.table_name));
        } else if (obj.EXTENSION() == null && obj.SCHEMA() == null && obj.DATABASE() == null
                && obj.CAST() == null && obj.SERVER() == null
                && (obj.DATA() == null || obj.WRAPPER() == null) && obj.EVENT() == null) {
            schema = getSchemaSafe(ids);
        }

        AbstractStatement st;
        DbObjType type;
        if (obj.function_args() != null && obj.ROUTINE() == null) {
            if (obj.PROCEDURE() != null) {
                type = DbObjType.PROCEDURE;
            } else if (obj.FUNCTION() != null) {
                type = DbObjType.FUNCTION;
            } else {
                type = DbObjType.AGGREGATE;
            }
            st = getSafe(PgSchema::getFunction, schema,
                    parseSignature(name, obj.function_args()), nameCtx.getStart());
        } else if (obj.OPERATOR() != null) {
            type = DbObjType.OPERATOR;
            Target_operatorContext targetOperCtx = obj.target_operator();
            st = getSafe(PgSchema::getOperator, schema,
                    parseOperatorSignature(nameCtx.getText(), targetOperCtx.operator_args()),
                    nameCtx.getStart());
        } else if (obj.EXTENSION() != null) {
            type = DbObjType.EXTENSION;
            st = getSafe(PgDatabase::getExtension, db, nameCtx);
        } else if (obj.EVENT() != null) {
            type = DbObjType.EVENT_TRIGGER;
            st = getSafe(PgDatabase::getEventTrigger, db, nameCtx);
        } else if (obj.FOREIGN() != null && obj.DATA() != null && obj.WRAPPER() != null) {
            type = DbObjType.FOREIGN_DATA_WRAPPER;
            st = getSafe(PgDatabase::getForeignDW, db, nameCtx);
        } else if (obj.SERVER() != null) {
            type = DbObjType.SERVER;
            st = getSafe(PgDatabase::getServer, db, nameCtx);
        } else if (obj.CONSTRAINT() != null) {
            List<ParserRuleContext> parentIds = getIdentifiers(obj.table_name);
            ParserRuleContext parentCtx = QNameParser.getFirstNameCtx(parentIds);
            type = DbObjType.CONSTRAINT;
            if (obj.DOMAIN() != null) {
                addObjReference(parentIds, DbObjType.DOMAIN, null);
                PgDomain domain = getSafe(PgSchema::getDomain, schema, parentCtx);
                st = getSafe(PgDomain::getConstraint, domain, nameCtx);
            } else {
                addObjReference(parentIds, DbObjType.TABLE, null);
                AbstractStatementContainer table = getSafe(PgSchema::getStatementContainer, schema, parentCtx);
                st = getSafe(AbstractStatementContainer::getConstraint, table, nameCtx);
            }
            ids = Arrays.asList(QNameParser.getSchemaNameCtx(parentIds), parentCtx, nameCtx);
        } else if (obj.CAST() != null) {
            Cast_nameContext castNameCtx = obj.cast_name();
            st = getSafe(PgDatabase::getCast, db, getCastName(castNameCtx), castNameCtx.getStart());
            type = DbObjType.CAST;
        } else if (obj.INDEX() != null) {
            type = DbObjType.INDEX;
            st = getSafe((sc, n) -> sc.getStatementContainers()
                            .flatMap(c -> Stream.concat(c.getIndexes().stream(), c.getConstraints().stream()))
                            .filter(s -> s.getName().equals(n))
                            .reduce((a, b) -> b.getStatementType() == DbObjType.INDEX ? b : a)
                            .orElse(null),
                    schema, nameCtx);
        } else if (obj.SCHEMA() != null && !Consts.PUBLIC.equals(name)) {
            type = DbObjType.SCHEMA;
            st = getSafe(PgDatabase::getSchema, db, nameCtx);
        } else if (obj.SEQUENCE() != null) {
            type = DbObjType.SEQUENCE;
            st = getSafe(PgSchema::getSequence, schema, nameCtx);
        } else if (obj.TABLE() != null) {
            type = DbObjType.TABLE;
            st = getSafe(PgSchema::getTable, schema, nameCtx);
        } else if (obj.VIEW() != null) {
            type = DbObjType.VIEW;
            st = getSafe(PgSchema::getView, schema, nameCtx);
        } else if (obj.TYPE() != null) {
            type = DbObjType.TYPE;
            st = getSafe(PgSchema::getType, schema, nameCtx);
        } else if (obj.DOMAIN() != null) {
            type = DbObjType.DOMAIN;
            st = getSafe(PgSchema::getDomain, schema, nameCtx);
        } else if (obj.COLLATION() != null) {
            type = DbObjType.COLLATION;
            st = getSafe(PgSchema::getCollation, schema, nameCtx);
        } else if ((obj.TRIGGER() != null && obj.EVENT() == null)
                || obj.POLICY() != null || obj.RULE() != null) {
            List<ParserRuleContext> parentIds = getIdentifiers(obj.table_name);
            addObjReference(parentIds, DbObjType.TABLE, null);
            ParserRuleContext tableCtx = QNameParser.getFirstNameCtx(parentIds);
            ids = Arrays.asList(QNameParser.getSchemaNameCtx(parentIds), tableCtx, nameCtx);
            AbstractStatementContainer c = getSafe(PgSchema::getStatementContainer, schema, tableCtx);
            if (obj.POLICY() != null) {
                type = DbObjType.POLICY;
                st = getSafe(AbstractStatementContainer::getPolicy, c, nameCtx);
            } else if (obj.RULE() != null) {
                type = DbObjType.RULE;
                st = getSafe(AbstractStatementContainer::getRule, c, nameCtx);
            } else {
                type = DbObjType.TRIGGER;
                st = getSafe(AbstractStatementContainer::getTrigger, c, nameCtx);
            }
        } else if (obj.CONFIGURATION() != null) {
            type = DbObjType.FTS_CONFIGURATION;
            st = getSafe(PgSchema::getFtsConfiguration, schema, nameCtx);
        } else if (obj.DICTIONARY() != null) {
            type = DbObjType.FTS_DICTIONARY;
            st = getSafe(PgSchema::getFtsDictionary, schema, nameCtx);
        } else if (obj.PARSER() != null) {
            type = DbObjType.FTS_PARSER;
            st = getSafe(PgSchema::getFtsParser, schema, nameCtx);
        } else if (obj.TEMPLATE() != null) {
            type = DbObjType.FTS_TEMPLATE;
            st = getSafe(PgSchema::getFtsTemplate, schema, nameCtx);
        } else if (obj.STATISTICS() != null) {
            type = DbObjType.STATISTICS;
            st = getSafe(PgSchema::getStatistics, schema, nameCtx);
        } else {
            addOutlineRefForCommentOrRule(ACTION_COMMENT, ctx);
            return;
        }

        doSafe(AbstractStatement::setComment, st, comment);
        if (type == DbObjType.FUNCTION || type == DbObjType.PROCEDURE || type == DbObjType.AGGREGATE) {
            addObjReference(ids, type, ACTION_ALTER, parseArguments(obj.function_args()));
        } else {
            addObjReference(ids, type, ACTION_COMMENT);
        }
    }

    @Override
    protected String getStmtAction() {
        return null;
    }
}
