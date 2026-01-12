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
package org.pgcodekeeper.core.database.pg.jdbc;

import org.pgcodekeeper.core.database.pg.loader.PgJdbcLoader;
import org.pgcodekeeper.core.database.base.jdbc.QueryBuilder;
import org.pgcodekeeper.core.database.base.schema.AbstractSchema;
import org.pgcodekeeper.core.database.pg.schema.PgFtsParser;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Reader for PostgreSQL full-text search parsers.
 * Loads full-text search parser definitions from pg_ts_parser system catalog.
 */
public final class PgFtsParsersReader extends PgAbstractSearchPathJdbcReader {

    /**
     * Constructs a new PgFtsParsersReader.
     *
     * @param loader the JDBC loader base instance
     */
    public PgFtsParsersReader(PgJdbcLoader loader) {
        super(loader);
    }

    @Override
    protected void processResult(ResultSet res, AbstractSchema schema) throws SQLException {
        PgFtsParser parser = new PgFtsParser(res.getString("prsname"));

        setFunctionWithDep(PgFtsParser::setStartFunction, parser, res.getString("prsstart"));
        setFunctionWithDep(PgFtsParser::setGetTokenFunction, parser, res.getString("prstoken"));
        setFunctionWithDep(PgFtsParser::setEndFunction, parser, res.getString("prsend"));
        setFunctionWithDep(PgFtsParser::setLexTypesFunction, parser, res.getString("prslextype"));

        String headline = res.getString("prsheadline");
        if (!"-".equals(headline)) {
            setFunctionWithDep(PgFtsParser::setHeadLineFunction, parser, headline);
        }

        loader.setComment(parser, res);
        loader.setAuthor(parser, res);
        schema.addChild(parser);
    }

    @Override
    public String getClassId() {
        return "pg_ts_parser";
    }

    @Override
    protected String getSchemaColumn() {
        return "res.prsnamespace";
    }

    @Override
    protected void fillQueryBuilder(QueryBuilder builder) {
        addDescriptionPart(builder);
        addExtensionDepsCte(builder);

        builder
                .column("res.prsname")
                .column("res.prsstart")
                .column("res.prstoken")
                .column("res.prsend")
                .column("res.prsheadline")
                .column("res.prslextype")
                .from("pg_catalog.pg_ts_parser res");
    }
}
