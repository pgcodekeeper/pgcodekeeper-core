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
import java.util.Locale;

import org.antlr.v4.runtime.CommonTokenStream;
import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.jdbc.*;
import org.pgcodekeeper.core.database.base.schema.*;
import org.pgcodekeeper.core.database.ch.loader.ChJdbcLoader;
import org.pgcodekeeper.core.database.ch.parser.statement.*;
import org.pgcodekeeper.core.database.ch.schema.*;
import org.pgcodekeeper.core.utils.Pair;

/**
 * Reader for ClickHouse relations.
 * Loads table, view, and dictionary definitions from system.tables table.
 */
public final class ChRelationsReader extends AbstractSearchPathJdbcReader<ChJdbcLoader> {

    /**
     * Creates a new ChRelationsReader.
     *
     * @param loader the JDBC loader instance
     */
    public ChRelationsReader(ChJdbcLoader loader) {
        super(loader);
    }

    @Override
    protected void processResult(ResultSet result, ISchema schema) throws SQLException {
        AbstractStatement child;
        String name = result.getString("name");
        String definition = result.getString("definition");

        var engineName = result.getString("engine").toLowerCase(Locale.ROOT);
        if (engineName.contains("dictionary")) {
            child = getDictionary(schema, name, definition);
        } else if (engineName.contains("view")) {
            // TODO remove it when added processing for refreshable materialized view
            if (definition.toLowerCase(Locale.ROOT).contains("refresh") && !definition.toLowerCase(Locale.ROOT).contains("live")) {
                return;
            }
            child = getView(schema, name, definition);
        } else {
            child = getTable(schema, name, definition, engineName);
        }

        schema.addChild(child);
    }

    private AbstractStatement getDictionary(ISchema schema, String name, String definition) {
        loader.setCurrentObject(new ObjectReference(schema.getName(), name, DbObjType.DICTIONARY));
        ChDictionary dict = new ChDictionary(name);
        loader.submitChAntlrTask(definition,
                p -> p.ch_file().query(0).stmt().ddl_stmt().create_stmt().create_dictinary_stmt(),
                ctx -> new ChCreateDictionary(ctx, (ChDatabase) schema.getDatabase(), loader.getSettings())
                        .parseObject(dict));
        return dict;
    }

    private AbstractStatement getView(ISchema schema, String name, String definition) {
        loader.setCurrentObject(new ObjectReference(schema.getName(), name, DbObjType.VIEW));
        ChView view = new ChView(name);
        loader.submitChAntlrTask(definition,
                p -> new Pair<>(
                        p.ch_file().query(0).stmt().ddl_stmt().create_stmt().create_view_stmt(),
                        (CommonTokenStream) p.getTokenStream()),
                pair -> new ChCreateView(pair.getFirst(), (ChDatabase) schema.getDatabase(), pair.getSecond(),
                        loader.getSettings())
                        .parseObject(view, true));
        return view;
    }

    private AbstractStatement getTable(ISchema schema, String name, String definition, String engineName) {
        loader.setCurrentObject(new ObjectReference(schema.getName(), name, DbObjType.TABLE));
        ChTable table;
        if (engineName.endsWith("log")) {
            table = new ChTableLog(name);
        } else {
            table = new ChTable(name);
        }
        loader.submitChAntlrTask(definition,
                p -> p.ch_file().query(0).stmt().ddl_stmt().create_stmt().create_table_stmt(),
                ctx -> new ChCreateTable(ctx, (ChDatabase) schema.getDatabase(), loader.getSettings())
                        .parseObject(table));
        return table;
    }

    @Override
    protected String getSchemaColumn() {
        return "res.database";
    }

    @Override
    protected void fillQueryBuilder(QueryBuilder builder) {
        builder
                .column("res.name")
                .column("res.create_table_query AS definition")
                .column("res.engine")
                .from("system.tables res")
                .where("notLike(res.name, '.inner_id.%')");
    }
}
