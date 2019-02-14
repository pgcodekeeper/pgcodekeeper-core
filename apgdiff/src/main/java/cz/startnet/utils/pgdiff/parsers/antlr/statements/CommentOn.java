package cz.startnet.utils.pgdiff.parsers.antlr.statements;

import java.util.Arrays;
import java.util.List;

import org.antlr.v4.runtime.ParserRuleContext;

import cz.startnet.utils.pgdiff.parsers.antlr.QNameParser;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Comment_on_statementContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.IdentifierContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Operator_nameContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Target_operatorContext;
import cz.startnet.utils.pgdiff.parsers.antlr.exception.UnresolvedReferenceException;
import cz.startnet.utils.pgdiff.schema.AbstractConstraint;
import cz.startnet.utils.pgdiff.schema.AbstractPgTable;
import cz.startnet.utils.pgdiff.schema.AbstractSchema;
import cz.startnet.utils.pgdiff.schema.AbstractTable;
import cz.startnet.utils.pgdiff.schema.PgColumn;
import cz.startnet.utils.pgdiff.schema.PgDatabase;
import cz.startnet.utils.pgdiff.schema.PgDomain;
import cz.startnet.utils.pgdiff.schema.PgObjLocation;
import cz.startnet.utils.pgdiff.schema.PgRuleContainer;
import cz.startnet.utils.pgdiff.schema.PgStatement;
import cz.startnet.utils.pgdiff.schema.PgTriggerContainer;
import cz.startnet.utils.pgdiff.schema.PgType;
import cz.startnet.utils.pgdiff.schema.PgView;
import cz.startnet.utils.pgdiff.schema.StatementActions;
import ru.taximaxim.codekeeper.apgdiff.ApgdiffConsts;
import ru.taximaxim.codekeeper.apgdiff.model.difftree.DbObjType;

public class CommentOn extends ParserAbstract {
    private final Comment_on_statementContext ctx;
    public CommentOn(Comment_on_statementContext ctx, PgDatabase db) {
        super(db);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        String comment = null;
        if (ctx.comment_text != null) {
            comment = ctx.comment_text.getText();
        }

        List<? extends ParserRuleContext> ids = null;
        ParserRuleContext nameCtx = null;
        String name = null;
        if (ctx.target_operator() == null) {
            ids = ctx.name.identifier();
        } else {
            Operator_nameContext operCtx = ctx.target_operator().name;
            ids = Arrays.asList(operCtx.schema_name, operCtx.operator);
        }

        nameCtx = QNameParser.getFirstNameCtx(ids);
        name = nameCtx.getText();

        DbObjType type = null;

        // column (separately because of schema qualification)
        // otherwise schema reference is considered unresolved
        if (ctx.COLUMN() != null) {
            if (isRefMode()) {
                return;
            }
            ParserRuleContext schemaCtx = QNameParser.getThirdNameCtx(ids);
            getSchemaNameSafe(ids);
            AbstractSchema schema = getSafe(PgDatabase::getSchema, db, schemaCtx);
            ParserRuleContext tableCtx = QNameParser.getSecondNameCtx(ids);
            if (tableCtx == null) {
                throw new UnresolvedReferenceException(
                        "Table name is missing for commented column!", nameCtx.getStart());
            }
            String tableName = tableCtx.getText();
            AbstractPgTable table = (AbstractPgTable) schema.getTable(tableName);
            if (table == null) {
                PgView view = (PgView) schema.getView(tableName);
                if (view == null) {
                    PgType t = ((PgType) getSafe(AbstractSchema::getType, schema, tableCtx));
                    t.getAttr(name).setComment(db.getArguments(), comment);
                } else {
                    view.addColumnComment(db.getArguments(), name, comment);
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
                column.setComment(db.getArguments(), comment);
            }
            return;
        }

        PgStatement st = null;
        AbstractSchema schema = null;
        if (ctx.TRIGGER() != null || ctx.RULE() != null || ctx.CONSTRAINT() != null) {
            schema = getSchemaSafe(ctx.table_name.identifier());
        } else if (ctx.EXTENSION() == null && ctx.SCHEMA() == null && ctx.DATABASE() == null) {
            schema = getSchemaSafe(ids);
        }

        if (ctx.FUNCTION() != null || ctx.AGGREGATE() != null) {
            type = ctx.FUNCTION() != null ? DbObjType.FUNCTION : DbObjType.AGGREGATE;
            st = getSafe(AbstractSchema::getFunction, schema,
                    parseSignature(name, ctx.function_args()), nameCtx.getStart());
        } else if (ctx.OPERATOR() != null) {
            type = DbObjType.OPERATOR;
            Target_operatorContext targetOperCtx = ctx.target_operator();
            st = getSafe(AbstractSchema::getOperator, schema,
                    parseSignature(targetOperCtx.name.operator.getText(),
                            targetOperCtx), targetOperCtx.getStart());
        } else if (ctx.EXTENSION() != null) {
            type = DbObjType.EXTENSION;
            st = getSafe(PgDatabase::getExtension, db, nameCtx);
        } else if (ctx.CONSTRAINT() != null && !isRefMode()) {
            List<IdentifierContext> parentIds = ctx.table_name.identifier();
            AbstractTable table = schema.getTable(QNameParser.getFirstName(parentIds));
            addObjReference(parentIds, DbObjType.TABLE, StatementActions.NONE);
            if (table == null) {
                PgDomain domain = getSafe(AbstractSchema::getDomain, schema, nameCtx);
                getSafe(PgDomain::getConstraint, domain, nameCtx).setComment(db.getArguments(), comment);
            } else {
                getSafe(AbstractTable::getConstraint, table, nameCtx).setComment(db.getArguments(), comment);
            }
        } else if (ctx.TRIGGER() != null) {
            type = DbObjType.TRIGGER;
            List<IdentifierContext> parentIds = ctx.table_name.identifier();
            ids = Arrays.asList(QNameParser.getSchemaNameCtx(parentIds),
                    QNameParser.getFirstNameCtx(parentIds), nameCtx);
            PgTriggerContainer c = getSafe(AbstractSchema::getTriggerContainer, schema,
                    QNameParser.getFirstNameCtx(ctx.table_name.identifier()));
            st = getSafe(PgTriggerContainer::getTrigger, c, nameCtx);
        } else if (ctx.DATABASE() != null) {
            st = db;
            type = DbObjType.DATABASE;
        } else if (ctx.INDEX() != null && !isRefMode()) {
            AbstractTable tab = schema.getTableByIndex(name);
            if (tab != null) {
                tab.getIndex(name).setComment(comment);
            } else {
                AbstractConstraint constr = null;
                for (AbstractTable table : schema.getTables()) {
                    constr = table.getConstraint(name);
                    if (constr != null) {
                        constr.setComment(db.getArguments(), comment);
                        break;
                    }
                }
                if (constr == null) {
                    throw new UnresolvedReferenceException(nameCtx.getStart());
                }
            }
        } else if (ctx.SCHEMA() != null && !ApgdiffConsts.PUBLIC.equals(name)) {
            type = DbObjType.SCHEMA;
            st = getSafe(PgDatabase::getSchema, db, nameCtx);
        } else if (ctx.SEQUENCE() != null) {
            type = DbObjType.SEQUENCE;
            st = getSafe(AbstractSchema::getSequence, schema, nameCtx);
        } else if (ctx.TABLE() != null) {
            type = DbObjType.TABLE;
            st = getSafe(AbstractSchema::getTable, schema, nameCtx);
        } else if (ctx.VIEW() != null) {
            type = DbObjType.VIEW;
            st = getSafe(AbstractSchema::getView, schema, nameCtx);
        } else if (ctx.TYPE() != null) {
            type = DbObjType.TYPE;
            st = getSafe(AbstractSchema::getType, schema, nameCtx);
        } else if (ctx.DOMAIN() != null) {
            type = DbObjType.DOMAIN;
            st = getSafe(AbstractSchema::getDomain, schema, nameCtx);
        } else if (ctx.RULE() != null) {
            type = DbObjType.RULE;
            List<IdentifierContext> parentIds = ctx.table_name.identifier();
            ids = Arrays.asList(QNameParser.getSchemaNameCtx(parentIds),
                    QNameParser.getFirstNameCtx(parentIds), nameCtx);
            PgRuleContainer c = getSafe(AbstractSchema::getRuleContainer, schema,
                    QNameParser.getFirstNameCtx(ctx.table_name.identifier()));
            st = getSafe(PgRuleContainer::getRule, c, nameCtx);
        } else if (ctx.CONFIGURATION() != null) {
            type = DbObjType.FTS_CONFIGURATION;
            st = getSafe(AbstractSchema::getFtsConfiguration, schema, nameCtx);
        } else if (ctx.DICTIONARY() != null) {
            type = DbObjType.FTS_DICTIONARY;
            st = getSafe(AbstractSchema::getFtsDictionary, schema, nameCtx);
        } else if (ctx.PARSER() != null) {
            type = DbObjType.FTS_PARSER;
            st = getSafe(AbstractSchema::getFtsParser, schema, nameCtx);
        } else if (ctx.TEMPLATE() != null) {
            type = DbObjType.FTS_TEMPLATE;
            st = getSafe(AbstractSchema::getFtsTemplate, schema, nameCtx);
        }

        if (type != null) {
            if (!isRefMode()) {
                st.setComment(db.getArguments(), comment);
            }
            PgObjLocation ref = addObjReference(ids, type, StatementActions.COMMENT);
            setCommentToDefinition(ref, comment);
        }
    }
}