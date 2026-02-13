/*******************************************************************************
 * Copyright 2017-2026 TAXTELECOM, LLC
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
package org.pgcodekeeper.core.database.ch.jdbc;

import java.sql.*;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.jdbc.*;
import org.pgcodekeeper.core.database.ch.loader.ChJdbcLoader;
import org.pgcodekeeper.core.database.ch.parser.generated.CHParser;
import org.pgcodekeeper.core.database.ch.parser.launcher.ChExpressionAnalysisLauncher;
import org.pgcodekeeper.core.database.ch.schema.*;

/**
 * Reader for ClickHouse policies.
 * Loads policy definitions from system.row_policies table.
 */
public class ChPoliciesReader extends AbstractJdbcReader<ChJdbcLoader> {

    private final ChDatabase db;

    /**
     * Creates a new ChPoliciesReader.
     *
     * @param loader the JDBC loader instance
     * @param db     the ClickHouse database to load policies into
     */
    public ChPoliciesReader(ChJdbcLoader loader, ChDatabase db) {
        super(loader);
        this.db = db;
    }

    @Override
    protected void processResult(ResultSet res) throws SQLException {
        String policyName = res.getString("name");

        loader.setCurrentObject(new ObjectReference(policyName, DbObjType.POLICY));

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
