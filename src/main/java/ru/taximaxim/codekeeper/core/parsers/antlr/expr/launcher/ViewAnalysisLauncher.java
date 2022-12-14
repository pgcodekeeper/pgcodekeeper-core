package ru.taximaxim.codekeeper.core.parsers.antlr.expr.launcher;

import java.util.Set;

import org.antlr.v4.runtime.ParserRuleContext;

import ru.taximaxim.codekeeper.core.loader.FullAnalyze;
import ru.taximaxim.codekeeper.core.parsers.antlr.SQLParser.Select_stmtContext;
import ru.taximaxim.codekeeper.core.parsers.antlr.expr.Select;
import ru.taximaxim.codekeeper.core.schema.PgObjLocation;
import ru.taximaxim.codekeeper.core.schema.PgView;
import ru.taximaxim.codekeeper.core.schema.meta.MetaContainer;
import ru.taximaxim.codekeeper.core.schema.meta.MetaUtils;

public class ViewAnalysisLauncher extends AbstractAnalysisLauncher {

    private FullAnalyze fullAnalyze;

    public ViewAnalysisLauncher(PgView stmt, Select_stmtContext ctx, String location) {
        super(stmt, ctx, location);
    }

    public void setFullAnalyze(FullAnalyze fullAnalyze) {
        this.fullAnalyze = fullAnalyze;
    }

    @Override
    public Set<PgObjLocation> analyze(ParserRuleContext ctx, MetaContainer meta) {
        Select select = new Select(meta);
        select.setFullAnaLyze(fullAnalyze);
        MetaUtils.initializeView(meta, stmt.getSchemaName(), stmt.getName(),
                select.analyze((Select_stmtContext) ctx));
        return select.getDepcies();
    }
}
