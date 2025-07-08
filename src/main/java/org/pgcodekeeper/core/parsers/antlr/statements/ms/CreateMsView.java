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
package org.pgcodekeeper.core.parsers.antlr.statements.ms;

import java.util.Arrays;
import java.util.List;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.expr.launcher.MsViewAnalysisLauncher;
import org.pgcodekeeper.core.parsers.antlr.generated.TSQLParser.Batch_statementContext;
import org.pgcodekeeper.core.parsers.antlr.generated.TSQLParser.Create_or_alter_viewContext;
import org.pgcodekeeper.core.parsers.antlr.generated.TSQLParser.Select_statementContext;
import org.pgcodekeeper.core.schema.ms.MsDatabase;
import org.pgcodekeeper.core.schema.ms.MsView;
import org.pgcodekeeper.core.settings.ISettings;

public final class CreateMsView extends BatchContextProcessor {

    private final Create_or_alter_viewContext ctx;

    private final boolean ansiNulls;
    private final boolean quotedIdentifier;

    public CreateMsView(Batch_statementContext ctx, MsDatabase db,
            boolean ansiNulls, boolean quotedIdentifier, CommonTokenStream stream, ISettings settings) {
        super(db, ctx, stream, settings);
        this.ctx = ctx.batch_statement_body().create_or_alter_view();
        this.ansiNulls = ansiNulls;
        this.quotedIdentifier = quotedIdentifier;
    }

    public CreateMsView(Create_or_alter_viewContext ctx, MsDatabase db,
            boolean ansiNulls, boolean quotedIdentifier, CommonTokenStream stream, ISettings settings) {
        super(db, ctx.getParent(), stream, settings);
        this.ctx = ctx;
        this.ansiNulls = ansiNulls;
        this.quotedIdentifier = quotedIdentifier;
    }

    @Override
    protected ParserRuleContext getDelimiterCtx() {
        return ctx.qualified_name();
    }

    @Override
    public void parseObject() {
        var qnameCtx = ctx.qualified_name();
        var nameCtx = qnameCtx.name;
        List<ParserRuleContext> ids = Arrays.asList(qnameCtx.schema, nameCtx);

        MsView view = new MsView(nameCtx.getText());
        fillObject(view);
        addSafe(getSchemaSafe(ids), view, ids);
    }

    public void fillObject(MsView view) {
        view.setAnsiNulls(ansiNulls);
        view.setQuotedIdentified(quotedIdentifier);

        for (var attribute : ctx.view_attribute()) {
            if (attribute.SCHEMABINDING() != null) {
                view.setSchemaBinding(true);
                break;
            }
        }

        setSourceParts(view);

        Select_statementContext vQuery = ctx.select_statement();
        if (vQuery != null) {
            db.addAnalysisLauncher(new MsViewAnalysisLauncher(view, vQuery, fileName));
        }
    }

    @Override
    protected String getStmtAction() {
        return getStrForStmtAction(ACTION_CREATE, DbObjType.VIEW, ctx.qualified_name());
    }
}
