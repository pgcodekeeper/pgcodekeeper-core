package cz.startnet.utils.pgdiff.parsers.antlr.statements;

import java.util.List;

import org.antlr.v4.runtime.tree.ParseTreeWalker;

import cz.startnet.utils.pgdiff.parsers.antlr.QNameParser;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Create_trigger_statementContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.IdentifierContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Names_referencesContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Schema_qualified_nameContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.When_triggerContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParserBaseListener;
import cz.startnet.utils.pgdiff.schema.GenericColumn;
import cz.startnet.utils.pgdiff.schema.PgDatabase;
import cz.startnet.utils.pgdiff.schema.PgSchema;
import cz.startnet.utils.pgdiff.schema.PgStatement;
import cz.startnet.utils.pgdiff.schema.PgTrigger;
import cz.startnet.utils.pgdiff.schema.PgTrigger.TgTypes;
import cz.startnet.utils.pgdiff.schema.PgTriggerContainer;
import ru.taximaxim.codekeeper.apgdiff.Log;
import ru.taximaxim.codekeeper.apgdiff.model.difftree.DbObjType;

public class CreateTrigger extends ParserAbstract {
    private final Create_trigger_statementContext ctx;

    public CreateTrigger(Create_trigger_statementContext ctx, PgDatabase db) {
        super(db);
        this.ctx = ctx;
    }

    @Override
    public PgStatement getObject() {
        List<IdentifierContext> ids = ctx.name.identifier();
        String name = QNameParser.getFirstName(ids);
        String schemaName = QNameParser.getSchemaName(ids, getDefSchemaName());
        PgTrigger trigger = new PgTrigger(name, getFullCtxText(ctx.getParent()));
        trigger.setTableName(ctx.tabl_name.getText());
        if (ctx.AFTER() != null) {
            trigger.setType(TgTypes.AFTER);
        } else if (ctx.BEFORE() != null) {
            trigger.setType(TgTypes.BEFORE);
        } else if (ctx.INSTEAD() != null) {
            trigger.setType(TgTypes.INSTEAD_OF);
        }
        if (ctx.ROW() != null) {
            trigger.setForEachRow(true);
        }
        if (ctx.STATEMENT() != null) {
            trigger.setForEachRow(false);
        }
        trigger.setOnDelete(ctx.delete_true != null);
        trigger.setOnInsert(ctx.insert_true != null);
        trigger.setOnUpdate(ctx.update_true != null);
        trigger.setOnTruncate(ctx.truncate_true != null);
        trigger.setFunction(getFullCtxText(ctx.func_name));

        List<IdentifierContext> funcIds = ctx.func_name.schema_qualified_name().identifier();
        trigger.addDep(new GenericColumn(QNameParser.getSchemaName(funcIds, getDefSchemaName()),
                QNameParser.getFirstName(funcIds) + "()", DbObjType.FUNCTION));

        for (Names_referencesContext column : ctx.names_references()) {
            for (Schema_qualified_nameContext nameCol : column.name) {
                String col = QNameParser.getFirstName(nameCol.identifier());
                trigger.addUpdateColumn(col);
                trigger.addDep(new GenericColumn(schemaName, trigger.getTableName(), col, DbObjType.COLUMN));
            }
        }
        WhenListener whenListener = new WhenListener();
        ParseTreeWalker.DEFAULT.walk(whenListener, ctx);
        trigger.setWhen(whenListener.getWhen());

        PgSchema schema = db.getSchema(schemaName);
        if (schema == null) {
            logSkipedObject(schemaName, "TRIGGER", trigger.getTableName());
            return null;
        } else {
            PgTriggerContainer c = schema.getTriggerContainer(trigger.getTableName());
            if (c != null){
                c.addTrigger(trigger);
            } else {
                Log.log(Log.LOG_ERROR,
                        new StringBuilder().append("trigger container ")
                        .append(trigger.getTableName())
                        .append(" not found on schema ").append(schemaName)
                        .append(" That's why trigger ").append(name)
                        .append("will be skipped").toString());
                return null;
            }
        }

        if (ctx.constaint_option() != null ) {
            trigger.setConstraint(true);
        } else {
            trigger.setConstraint(false);
        }

        return trigger;
    }

    public static class WhenListener extends SQLParserBaseListener {
        private String when;

        @Override
        public void exitWhen_trigger(When_triggerContext ctx) {
            when = getFullCtxText(ctx.when_expr);
        }

        public String getWhen() {
            return when;
        }
    }
}
