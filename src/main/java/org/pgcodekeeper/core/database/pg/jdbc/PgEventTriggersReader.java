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

import java.sql.*;

import org.pgcodekeeper.core.database.api.schema.*;
import org.pgcodekeeper.core.database.base.jdbc.QueryBuilder;
import org.pgcodekeeper.core.database.pg.loader.PgJdbcLoader;
import org.pgcodekeeper.core.database.pg.schema.*;
import org.pgcodekeeper.core.utils.Utils;

/**
 * Reader for PostgreSQL event triggers.
 * Loads event trigger definitions from pg_event_trigger system catalog.
 */
public final class PgEventTriggersReader extends PgAbstractJdbcReader {

    private final PgDatabase db;

    /**
     * Creates a new event triggers reader.
     *
     * @param loader the JDBC loader for database access
     * @param db     the PostgreSQL database to populate with event triggers
     */
    public PgEventTriggersReader(PgJdbcLoader loader, PgDatabase db) {
        super(loader);
        this.db = db;
    }

    @Override
    protected void processResult(ResultSet res) throws SQLException {
        String evtName = res.getString("evtname");
        loader.setCurrentObject(new ObjectReference(evtName, DbObjType.EVENT_TRIGGER));

        PgEventTrigger evt = new PgEventTrigger(evtName);
        evt.setEvent(res.getString("evtevent"));

        switch (res.getString("evtenabled")) {
            case "D":
                evt.setMode("DISABLE");
                break;
            case "R":
                evt.setMode("ENABLE REPLICA");
                break;
            case "A":
                evt.setMode("ENABLE ALWAYS");
                break;
        }

        String[] tags = PgJdbcUtils.getColArray(res, "evttags", true);
        if (tags != null) {
            for (String tag : tags) {
                evt.addTag(Utils.quoteString(tag));
            }
        }

        String funcName = res.getString("proname");
        String funcSchema = res.getString("nspname");
        evt.setExecutable(funcSchema + '.' + funcName + "()");
        evt.addDependency(new ObjectReference(funcSchema, funcName + "()", DbObjType.FUNCTION));

        loader.setOwner(evt, res.getString("rolname"));
        loader.setComment(evt, res);
        loader.setAuthor(evt, res);
        db.addChild(evt);
    }

    @Override
    protected void fillQueryBuilder(QueryBuilder builder) {
        addDescriptionPart(builder);
        addExtensionDepsCte(builder);

        builder.column("res.evtname")
                .column("res.evtevent")
                .column("res.evtenabled")
                .column("res.evttags")
                .column("nsp.nspname")
                .column("p.proname")
                .column("o.rolname")
                .from("pg_catalog.pg_event_trigger res")
                .join("JOIN pg_catalog.pg_roles o ON o.oid = res.evtowner")
                .join("JOIN pg_catalog.pg_proc p ON p.oid = res.evtfoid")
                .join("JOIN pg_catalog.pg_namespace nsp ON p.pronamespace = nsp.oid");
    }

    @Override
    public String getClassId() {
        return "pg_event_trigger";
    }
}
