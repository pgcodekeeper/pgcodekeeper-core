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

import java.sql.ResultSet;
import java.sql.SQLException;

import org.pgcodekeeper.core.PgDiffUtils;
import org.pgcodekeeper.core.loader.QueryBuilder;
import org.pgcodekeeper.core.loader.jdbc.JdbcLoaderBase;
import org.pgcodekeeper.core.loader.jdbc.JdbcReader;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.parsers.antlr.statements.ParserAbstract;
import org.pgcodekeeper.core.schema.AbstractSchema;
import org.pgcodekeeper.core.schema.pg.PgFtsDictionary;

public final class FtsDictionariesReader extends JdbcReader {

    public FtsDictionariesReader(JdbcLoaderBase loader) {
        super(loader);
    }

    @Override
    protected void processResult(ResultSet res, AbstractSchema schema) throws SQLException {
        String name = res.getString("dictname");
        PgFtsDictionary dic = new PgFtsDictionary(name);

        String options = res.getString("dictinitoption");
        if (options != null) {
            ParserAbstract.fillOptionParams(options.replace(" = ", "=").split(", "), dic::addOption, false, false, true);
        }

        loader.setOwner(dic, res.getLong("dictowner"));

        String tmplname = res.getString("tmplname");
        String templateSchema = res.getString("tmplnspname");

        addDep(dic, templateSchema, tmplname, DbObjType.FTS_TEMPLATE);

        dic.setTemplate(PgDiffUtils.getQuotedName(templateSchema) + '.'
                + PgDiffUtils.getQuotedName(tmplname));

        // COMMENT
        loader.setComment(dic, res);
        loader.setAuthor(dic, res);
        schema.addChild(dic);
    }

    @Override
    protected String getClassId() {
        return "pg_ts_dict";
    }

    @Override
    protected String getSchemaColumn() {
        return "res.dictnamespace";
    }

    @Override
    protected void fillQueryBuilder(QueryBuilder builder) {
        addExtensionDepsCte(builder);
        addDescriptionPart(builder);

        builder
        .column("res.dictname")
        .column("res.dictowner::bigint")
        .column("t.tmplname")
        .column("n.nspname AS tmplnspname")
        .column("res.dictinitoption")
        .from("pg_catalog.pg_ts_dict res")
        .join("LEFT JOIN pg_catalog.pg_ts_template t ON res.dicttemplate = t.oid")
        .join("LEFT JOIN pg_catalog.pg_namespace n ON t.tmplnamespace = n.oid");
    }
}
