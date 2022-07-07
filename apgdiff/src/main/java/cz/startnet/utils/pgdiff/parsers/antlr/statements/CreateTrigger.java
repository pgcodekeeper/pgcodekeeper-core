package cz.startnet.utils.pgdiff.parsers.antlr.statements;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.antlr.v4.runtime.ParserRuleContext;

import cz.startnet.utils.pgdiff.PgDiffUtils;
import cz.startnet.utils.pgdiff.parsers.antlr.QNameParser;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Create_trigger_statementContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.IdentifierContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Identifier_listContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Schema_qualified_name_nontypeContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Table_deferrableContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Table_initialy_immedContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.Trigger_referencingContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.VexContext;
import cz.startnet.utils.pgdiff.parsers.antlr.SQLParser.When_triggerContext;
import cz.startnet.utils.pgdiff.parsers.antlr.expr.launcher.TriggerAnalysisLauncher;
import cz.startnet.utils.pgdiff.schema.AbstractSchema;
import cz.startnet.utils.pgdiff.schema.PgDatabase;
import cz.startnet.utils.pgdiff.schema.PgStatementContainer;
import cz.startnet.utils.pgdiff.schema.PgTrigger;
import cz.startnet.utils.pgdiff.schema.PgTrigger.TgTypes;
import ru.taximaxim.codekeeper.apgdiff.model.difftree.DbObjType;

public class CreateTrigger extends ParserAbstract {
    private final Create_trigger_statementContext ctx;
    public CreateTrigger(Create_trigger_statementContext ctx, PgDatabase db) {
        super(db);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        List<ParserRuleContext> ids = getIdentifiers(ctx.table_name);
        addObjReference(ids, DbObjType.TABLE, null);

        PgTrigger trigger = new PgTrigger(ctx.name.getText());
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

        if (ctx.CONSTRAINT() != null ) {
            trigger.setConstraint(true);
            Table_deferrableContext  def  = ctx.table_deferrable();
            if (def != null && def.NOT() == null){
                Table_initialy_immedContext  initImmed  = ctx.table_initialy_immed();
                if (initImmed != null){
                    trigger.setImmediate(initImmed.DEFERRED() == null);
                }
            }

            if (ctx.referenced_table_name != null) {
                List<ParserRuleContext> refName = getIdentifiers(ctx.referenced_table_name);
                String refSchemaName = QNameParser.getSecondName(refName);
                String refRelName = QNameParser.getFirstName(refName);

                StringBuilder sb = new StringBuilder();
                if (refSchemaName == null) {
                    refSchemaName = getSchemaNameSafe(ids);
                }

                if (refSchemaName != null) {
                    sb.append(PgDiffUtils.getQuotedName(refSchemaName)).append('.');
                }
                sb.append(PgDiffUtils.getQuotedName(refRelName));

                addDepSafe(trigger, refName, DbObjType.TABLE, true);
                trigger.setRefTableName(sb.toString());
            }
        }

        for (Trigger_referencingContext ref : ctx.trigger_referencing()) {
            String name = ref.identifier().getText();
            if (ref.NEW() != null) {
                trigger.setNewTable(name);
            } else {
                trigger.setOldTable(name);
            }
        }

        Schema_qualified_name_nontypeContext funcNameCtx = ctx.func_name
                .schema_qualified_name_nontype();
        if (funcNameCtx.schema != null) {
            // TODO add empty signature to function name
            // when function signatures in refs and defs will be supported
            addDepSafe(trigger, getIdentifiers(funcNameCtx), DbObjType.FUNCTION, true, "()");
        }

        ParserRuleContext schemaCtx = QNameParser.getSchemaNameCtx(ids);
        ParserRuleContext parentCtx = QNameParser.getFirstNameCtx(ids);

        for (Identifier_listContext column : ctx.identifier_list()) {
            for (IdentifierContext nameCol : column.identifier()) {
                trigger.addUpdateColumn(nameCol.getText());
                addDepSafe(trigger, Arrays.asList(schemaCtx, parentCtx, nameCol),
                        DbObjType.COLUMN, true);
            }
        }
        parseWhen(ctx.when_trigger(), trigger, db, fileName);

        PgStatementContainer cont = getSafe(AbstractSchema::getStatementContainer,
                getSchemaSafe(ids), parentCtx);
        addSafe(cont, trigger, Arrays.asList(schemaCtx, parentCtx, ctx.name));
    }

    public static void parseWhen(When_triggerContext whenCtx, PgTrigger trigger,
            PgDatabase db, String location) {
        if (whenCtx != null) {
            VexContext vex = whenCtx.vex();
            trigger.setWhen(getFullCtxText(vex));
            db.addAnalysisLauncher(new TriggerAnalysisLauncher(trigger, vex, location));
        }
    }

    @Override
    protected String getStmtAction() {
        List<ParserRuleContext> ids = new ArrayList<>(getIdentifiers(ctx.table_name));
        ids.add(ctx.name);
        return getStrForStmtAction(ACTION_CREATE, DbObjType.TRIGGER, ids);
    }
}
