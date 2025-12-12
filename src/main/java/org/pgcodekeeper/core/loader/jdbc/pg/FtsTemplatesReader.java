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
package org.pgcodekeeper.core.loader.jdbc.pg;

import org.pgcodekeeper.core.loader.QueryBuilder;
import org.pgcodekeeper.core.loader.jdbc.JdbcLoaderBase;
import org.pgcodekeeper.core.loader.jdbc.JdbcReader;
import org.pgcodekeeper.core.database.base.schema.AbstractSchema;
import org.pgcodekeeper.core.database.pg.schema.PgFtsTemplate;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Reader for PostgreSQL full-text search templates.
 * Loads full-text search template definitions from pg_ts_template system catalog.
 */
public final class FtsTemplatesReader extends JdbcReader {

    /**
     * Constructs a new FtsTemplatesReader.
     *
     * @param loader the JDBC loader base instance
     */
    public FtsTemplatesReader(JdbcLoaderBase loader) {
        super(loader);
    }

    @Override
    protected void processResult(ResultSet res, AbstractSchema schema) throws SQLException {
        PgFtsTemplate template = new PgFtsTemplate(res.getString("tmplname"));

        String init = res.getString("tmplinit");
        if (!"-".equals(init)) {
            setFunctionWithDep(PgFtsTemplate::setInitFunction, template, init);
        }

        setFunctionWithDep(PgFtsTemplate::setLexizeFunction,
                template, res.getString("tmpllexize"));

        // COMMENT
        loader.setComment(template, res);
        loader.setAuthor(template, res);
        schema.addChild(template);
    }

    @Override
    protected String getClassId() {
        return "pg_ts_template";
    }

    @Override
    protected String getSchemaColumn() {
        return "res.tmplnamespace";
    }

    @Override
    protected void fillQueryBuilder(QueryBuilder builder) {
        addExtensionDepsCte(builder);
        addDescriptionPart(builder);

        builder
                .column("res.tmplname")
                .column("res.tmplinit")
                .column("res.tmpllexize")
                .from("pg_catalog.pg_ts_template res");
    }
}
