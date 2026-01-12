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
package org.pgcodekeeper.core.database.pg.jdbc;

import org.pgcodekeeper.core.database.api.schema.DbObjType;
import org.pgcodekeeper.core.database.api.schema.GenericColumn;
import org.pgcodekeeper.core.database.base.jdbc.AbstractSearchPathJdbcReader;
import org.pgcodekeeper.core.database.base.jdbc.QueryBuilder;
import org.pgcodekeeper.core.database.base.schema.AbstractStatement;
import org.pgcodekeeper.core.database.pg.PgDiffUtils;
import org.pgcodekeeper.core.database.pg.loader.PgJdbcLoader;
import org.pgcodekeeper.core.utils.Utils;

import java.util.function.BiConsumer;

public abstract class PgAbstractSearchPathJdbcReader extends AbstractSearchPathJdbcReader<PgJdbcLoader> implements IPgJdbcReader {

    protected static final String EXTENSIONS_SCHEMAS = "extensions_schemas";

    private static final QueryBuilder EXTENSION_SCHEMA_CTE_SUBSELECT = new QueryBuilder()
            .column("1")
            .from("pg_catalog.pg_depend dp")
            .where("dp.objid = n.oid")
            .where("dp.deptype = 'e'")
            .where("dp.classid = 'pg_catalog.pg_namespace'::pg_catalog.regclass");

    protected static final QueryBuilder EXTENSION_SCHEMA_CTE = new QueryBuilder()
            .column("n.oid")
            .from("pg_catalog.pg_namespace n")
            .where("EXISTS", EXTENSION_SCHEMA_CTE_SUBSELECT);

    protected final String classId;

    protected PgAbstractSearchPathJdbcReader(PgJdbcLoader loader) {
        super(loader);
        String tmpClassId = getClassId();
        this.classId = tmpClassId == null ? null : Utils.quoteString(PG_CATALOG + tmpClassId);
    }

    protected String getTextWithCheckNewLines(String text) {
        return Utils.checkNewLines(text, loader.getSettings().isKeepNewlines());
    }

    protected void addDep(AbstractStatement statement, String schemaName, String name, DbObjType type) {
        if (schemaName != null && !PgDiffUtils.isSystemSchema(schemaName)) {
            statement.addDependency(new GenericColumn(schemaName, DbObjType.SCHEMA));
            statement.addDependency(new GenericColumn(schemaName, name, type));
        }
    }

    /**
     * @deprecated {@link #setFunctionWithDep(BiConsumer, AbstractStatement, String, String)}
     */
    @Deprecated
    protected <T extends AbstractStatement> void setFunctionWithDep(
            BiConsumer<T, String> setter, T statement, String function) {
        setFunctionWithDep(setter, statement, function, null);
    }

    @Override
    public QueryBuilder makeQuery() {
        var builder = super.makeQuery();
        appendExtension(builder, loader.getExtensionSchema());
        return builder;
    }

    @Override
    public String getFormattedClassId() {
        return classId;
    }
}
