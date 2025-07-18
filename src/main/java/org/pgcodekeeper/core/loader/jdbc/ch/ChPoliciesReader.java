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
package org.pgcodekeeper.core.loader.jdbc.ch;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.pgcodekeeper.core.loader.QueryBuilder;
import org.pgcodekeeper.core.loader.jdbc.AbstractStatementReader;
import org.pgcodekeeper.core.loader.jdbc.JdbcLoaderBase;
import org.pgcodekeeper.core.loader.jdbc.XmlReaderException;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.expr.launcher.ChExpressionAnalysisLauncher;
import org.pgcodekeeper.core.parsers.antlr.generated.CHParser;
import org.pgcodekeeper.core.schema.GenericColumn;
import org.pgcodekeeper.core.schema.ch.ChDatabase;
import org.pgcodekeeper.core.schema.ch.ChPolicy;

public class ChPoliciesReader extends AbstractStatementReader {

    private final ChDatabase db;

    public ChPoliciesReader(JdbcLoaderBase loader, ChDatabase db) {
        super(loader);
        this.db = db;
    }

    @Override
    protected void processResult(ResultSet res) throws SQLException, XmlReaderException {
        String policyName = res.getString("name");

        loader.setCurrentObject(new GenericColumn(policyName, DbObjType.POLICY));

        ChPolicy p = new ChPolicy(policyName);

        ChJdbcUtils.addRoles(res, "apply_to_list", "apply_to_except", p, ChPolicy::addRole, ChPolicy::addExcept);

        p.setPermissive(!res.getBoolean("is_restrictive"));

        String using = res.getString("select_filter");
        if (using != null) {
            loader.submitChAntlrTask(using, CHParser::expr_eof, ctx -> db.addAnalysisLauncher(
                    new ChExpressionAnalysisLauncher(p, ctx.expr(), loader.getCurrentLocation())));
            p.setUsing(using);
        }

        db.addChild(p);
    }

    @Override
    protected void fillQueryBuilder(QueryBuilder builder) {
        builder
        .column("res.name")
        .column("res.is_restrictive")
        .column("res.select_filter")
        .column("res.apply_to_list")
        .column("res.apply_to_except")
        .from("system.row_policies res");
    }
}
