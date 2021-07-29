package cz.startnet.utils.pgdiff.parsers.antlr.statements;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.antlr.v4.runtime.ParserRuleContext;

import cz.startnet.utils.pgdiff.parsers.antlr.QNameParser;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.All_simple_opContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Alter_owner_statementContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.IdentifierContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Operator_nameContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Owner_toContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Target_operatorContext;
import cz.startnet.utils.pgdiff.schema.AbstractSchema;
import cz.startnet.utils.pgdiff.schema.PgDatabase;
import cz.startnet.utils.pgdiff.schema.PgStatement;
import cz.startnet.utils.pgdiff.schema.StatementOverride;
import ru.taximaxim.codekeeper.apgdiff.ApgdiffConsts;
import ru.taximaxim.codekeeper.apgdiff.model.difftree.DbObjType;

public class AlterOwner extends ParserAbstract {

    private final Alter_owner_statementContext ctx;
    private final Map<PgStatement, StatementOverride> overrides;

    public AlterOwner(Alter_owner_statementContext ctx, PgDatabase db) {
        this(ctx, db, null);
    }

    public AlterOwner(Alter_owner_statementContext ctx, PgDatabase db,
            Map<PgStatement, StatementOverride> overrides) {
        super(db);
        this.ctx = ctx;
        this.overrides = overrides;
    }

    @Override
    public void parseObject() {
        Owner_toContext owner = ctx.owner_to();

        if (db.getArguments().isIgnorePrivileges() || owner.name == null) {
            return;
        }

        PgStatement st = null;

        if (ctx.OPERATOR() != null) {
            Target_operatorContext targetOperCtx = ctx.target_operator();
            Operator_nameContext operNameCtx = targetOperCtx.name;
            IdentifierContext schemaCtx = operNameCtx.schema_name;
            All_simple_opContext nameCtx = operNameCtx.operator;
            List<ParserRuleContext> ids = Arrays.asList(schemaCtx, nameCtx);
            st = getSafe(AbstractSchema::getOperator, getSchemaSafe(ids),
                    parseSignature(nameCtx.getText(), targetOperCtx),
                    nameCtx.getStart());
            setOwner(st, owner);
            addObjReference(ids, DbObjType.OPERATOR, ACTION_ALTER);
            return;
        }

        List<IdentifierContext> ids = ctx.name.identifier();
        IdentifierContext nameCtx = QNameParser.getFirstNameCtx(ids);

        DbObjType type = null;
        if (ctx.SCHEMA() != null) {
            st = getSafe(PgDatabase::getSchema, db, nameCtx);
            type = DbObjType.SCHEMA;
        } else {
            if (ctx.DOMAIN() != null) {
                st = getSafe(AbstractSchema::getDomain, getSchemaSafe(ids), nameCtx);
                type = DbObjType.DOMAIN;
            } else if (ctx.VIEW() != null) {
                st = getSafe(AbstractSchema::getView, getSchemaSafe(ids), nameCtx);
                type = DbObjType.VIEW;
            } else if (ctx.FOREIGN() != null && ctx.DATA() != null && ctx.WRAPPER() != null ) {
                st = getSafe(PgDatabase::getForeignDW, db, nameCtx);
                type = DbObjType.FOREIGN_DATA_WRAPPER;
            } else if (ctx.SERVER() != null ) {
                st = getSafe(PgDatabase::getServer, db, nameCtx);
                type = DbObjType.SERVER;
            } else if (ctx.DICTIONARY() != null) {
                st = getSafe(AbstractSchema::getFtsDictionary, getSchemaSafe(ids), nameCtx);
                type = DbObjType.FTS_DICTIONARY;
            } else if (ctx.CONFIGURATION() != null) {
                st = getSafe(AbstractSchema::getFtsConfiguration, getSchemaSafe(ids), nameCtx);
                type = DbObjType.FTS_CONFIGURATION;
            } else if (ctx.SEQUENCE() != null) {
                st = getSafe(AbstractSchema::getSequence, getSchemaSafe(ids), nameCtx);
                type = DbObjType.SEQUENCE;
            } else if (ctx.TYPE() != null) {
                st = getSafe(AbstractSchema::getType, getSchemaSafe(ids), nameCtx);
                type = DbObjType.TYPE;
            } else if (ctx.PROCEDURE() != null || ctx.FUNCTION() != null || ctx.AGGREGATE() != null) {
                st = getSafe(AbstractSchema::getFunction, getSchemaSafe(ids), parseSignature(nameCtx.getText(),
                        ctx.function_args()), nameCtx.getStart());
                if (ctx.FUNCTION() != null) {
                    type = DbObjType.FUNCTION;
                } else if (ctx.PROCEDURE() != null) {
                    type = DbObjType.PROCEDURE;
                } else {
                    type = DbObjType.AGGREGATE;
                }
            }
        }

        if (type != null)  {
            addObjReference(ids, type, ACTION_ALTER);
        }

        if (st == null || (type == DbObjType.SCHEMA
                && ApgdiffConsts.PUBLIC.equals(nameCtx.getText())
                && "postgres".equals(owner.name.getText()))) {
            return;
        }

        setOwner(st, owner);
    }

    private void setOwner(PgStatement st, Owner_toContext owner) {
        if (overrides == null) {
            fillOwnerTo(owner, st);
        } else {
            overrides.computeIfAbsent(st,
                    k -> new StatementOverride()).setOwner(owner.name.getText());
        }
    }

    @Override
    protected String getStmtAction() {
        DbObjType type = null;
        if (ctx.SCHEMA() != null) {
            type = DbObjType.SCHEMA;
        } else if (ctx.FOREIGN() != null && ctx.DATA() != null && ctx.WRAPPER() != null) {
            type = DbObjType.FOREIGN_DATA_WRAPPER;
        } else if (ctx.SERVER() != null) {
            type = DbObjType.SERVER;
        } else if (ctx.DOMAIN() != null) {
            type = DbObjType.DOMAIN;
        } else if (ctx.VIEW() != null) {
            type = DbObjType.VIEW;
        } else if (ctx.DICTIONARY() != null) {
            type = DbObjType.FTS_DICTIONARY;
        } else if (ctx.CONFIGURATION() != null) {
            type = DbObjType.FTS_CONFIGURATION;
        } else if (ctx.SEQUENCE() != null) {
            type = DbObjType.SEQUENCE;
        } else if (ctx.TYPE() != null) {
            type = DbObjType.TYPE;
        } else if (ctx.FUNCTION() != null) {
            type = DbObjType.FUNCTION;
        } else if (ctx.PROCEDURE() != null) {
            type = DbObjType.PROCEDURE;
        } else if (ctx.AGGREGATE() != null) {
            type = DbObjType.AGGREGATE;
        } else if (ctx.OPERATOR() != null) {
            type = DbObjType.OPERATOR;
        } else {
            return null;
        }

        String schemaName = null;
        String objName = null;
        Target_operatorContext targetOperCtx;
        if (ctx.name != null) {
            List<IdentifierContext> ids = ctx.name.identifier();
            schemaName = QNameParser.getSchemaName(ids);
            objName = QNameParser.getFirstName(ids);
        } else if ((targetOperCtx = ctx.target_operator()) != null) {
            Operator_nameContext operNameCtx = targetOperCtx.name;
            schemaName = operNameCtx.schema_name.getText();
            objName = operNameCtx.operator.getText();
        } else {
            return null;
        }

        StringBuilder sb = new StringBuilder(ACTION_ALTER);
        sb.append(' ').append(type).append(' ');
        if (type != DbObjType.SCHEMA) {
            sb.append(schemaName).append('.');
        }
        return sb.append(objName).toString();
    }
}
