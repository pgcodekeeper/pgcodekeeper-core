package ru.taximaxim.codekeeper.core.parsers.antlr.statements.mssql;

import java.util.Arrays;

import org.antlr.v4.runtime.ParserRuleContext;

import ru.taximaxim.codekeeper.core.model.difftree.DbObjType;
import ru.taximaxim.codekeeper.core.parsers.antlr.TSQLParser.Enable_disable_triggerContext;
import ru.taximaxim.codekeeper.core.parsers.antlr.TSQLParser.IdContext;
import ru.taximaxim.codekeeper.core.parsers.antlr.TSQLParser.Names_referencesContext;
import ru.taximaxim.codekeeper.core.parsers.antlr.TSQLParser.Qualified_nameContext;
import ru.taximaxim.codekeeper.core.parsers.antlr.statements.ParserAbstract;
import ru.taximaxim.codekeeper.core.schema.AbstractSchema;
import ru.taximaxim.codekeeper.core.schema.MsTrigger;
import ru.taximaxim.codekeeper.core.schema.PgDatabase;
import ru.taximaxim.codekeeper.core.schema.PgObjLocation;
import ru.taximaxim.codekeeper.core.schema.PgStatementContainer;

public class DisableMsTrigger extends ParserAbstract {

    private final Enable_disable_triggerContext ctx;

    public DisableMsTrigger(Enable_disable_triggerContext ctx, PgDatabase db) {
        super(db);
        this.ctx = ctx;
    }

    @Override
    public void parseObject() {
        Names_referencesContext triggers = ctx.names_references();
        Qualified_nameContext parent = ctx.qualified_name();
        if (triggers == null || parent == null) {
            return;
        }

        IdContext schemaCtx = parent.schema;
        PgStatementContainer cont = getSafe(AbstractSchema::getStatementContainer,
                getSchemaSafe(Arrays.asList(schemaCtx, parent.name)), parent.name);
        addObjReference(Arrays.asList(parent.schema, parent.name),
                DbObjType.TABLE, null);

        for (Qualified_nameContext qname : triggers.qualified_name()) {
            MsTrigger trig = (MsTrigger) getSafe(PgStatementContainer::getTrigger,
                    cont, qname.name);
            addObjReference(Arrays.asList(schemaCtx, parent.name, qname.name),
                    DbObjType.TRIGGER, ACTION_ALTER);
            if (ctx.DISABLE() != null) {
                doSafe(MsTrigger::setDisable, trig, true);
            }
        }
    }

    @Override
    protected PgObjLocation fillQueryLocation(ParserRuleContext ctx) {
        StringBuilder sb = new StringBuilder();
        Enable_disable_triggerContext ctxEnableDisableTr = (Enable_disable_triggerContext) ctx;
        sb.append(ctxEnableDisableTr.DISABLE() != null ? "DISABLE " : "ENABLE ")
        .append("TRIGGER");

        Names_referencesContext triggers = ctxEnableDisableTr.names_references();
        Qualified_nameContext parent = ctxEnableDisableTr.qualified_name();

        if (triggers != null && parent != null) {
            sb.append(' ');

            String schemaName = parent.schema.getText();
            String parentName = parent.name.getText();

            for (Qualified_nameContext qname : triggers.qualified_name()) {
                sb.append(schemaName)
                .append('.').append(parentName)
                .append('.').append(qname.name.getText())
                .append(", ");
            }

            sb.setLength(sb.length() - 2);
        }

        PgObjLocation loc = new PgObjLocation.Builder()
                .setAction(sb.toString())
                .setCtx(ctx)
                .setSql(getFullCtxText(ctx))
                .build();

        db.addReference(fileName, loc);
        return loc;
    }

    @Override
    protected String getStmtAction() {
        return null;
    }
}
