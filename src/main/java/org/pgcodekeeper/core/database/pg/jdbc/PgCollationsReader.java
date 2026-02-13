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
import org.pgcodekeeper.core.database.pg.schema.PgCollation;
import org.pgcodekeeper.core.utils.Utils;

/**
 * Reader for PostgreSQL collations.
 * Loads collation definitions from pg_collation system catalog.
 */
public final class PgCollationsReader extends PgAbstractSearchPathJdbcReader {

    /**
     * Creates a new collations reader.
     *
     * @param loader the JDBC loader base for database operations
     */
    public PgCollationsReader(PgJdbcLoader loader) {
        super(loader);
    }

    @Override
    protected void processResult(ResultSet res, ISchema schema) throws SQLException {
        String schemaName = schema.getName();
        String collName = res.getString("collname");
        loader.setCurrentObject(new ObjectReference(schemaName, collName, DbObjType.COLLATION));

        PgCollation coll = new PgCollation(collName);

        String lcCollate = res.getString("collcollate");
        if (lcCollate != null) {
            coll.setLcCollate(Utils.quoteString(lcCollate));
        }
        String lcCtype = res.getString("collctype");
        if (lcCtype != null) {
            coll.setLcCtype(Utils.quoteString(lcCtype));
        }

        loader.setComment(coll, res);

        if (PgSupportedVersion.GP_VERSION_7.isLE(loader.getVersion())) {
            String provider = res.getString("collprovider");
            switch (provider) {
                case "c":
                    coll.setProvider("libc");
                    break;
                case "i":
                    coll.setProvider("icu");
                    String locale = null;
                    if (PgSupportedVersion.VERSION_17.isLE(loader.getVersion())) {
                        locale = res.getString("colllocale");
                    } else if (PgSupportedVersion.VERSION_15.isLE(loader.getVersion())) {
                        locale = res.getString("colliculocale");
                    }
                    if (locale != null) {
                        String quotedLocale = Utils.quoteString(locale);
                        coll.setLcCollate(quotedLocale);
                        coll.setLcCtype(quotedLocale);
                    }
                    break;
                case "d":
                    coll.setProvider("default");
                    break;
            }

            coll.setDeterministic(res.getBoolean("collisdeterministic"));
        }

        if (PgSupportedVersion.VERSION_16.isLE(loader.getVersion())) {
            String rules = res.getString("collicurules");
            if (rules != null) {
                coll.setRules(Utils.quoteString(rules));
            }
        }

        loader.setOwner(coll, res.getLong("collowner"));
        loader.setAuthor(coll, res);

        schema.addChild(coll);
    }

    @Override
    public String getClassId() {
        return "pg_collation";
    }

    @Override
    protected String getSchemaColumn() {
        return "res.collnamespace";
    }

    @Override
    protected void fillQueryBuilder(QueryBuilder builder) {
        addExtensionDepsCte(builder);
        addDescriptionPart(builder);

        builder
                .column("res.collname")
                .column("res.collcollate")
                .column("res.collctype")
                .column("res.collowner::bigint")
                .from("pg_catalog.pg_collation res");

        if (PgSupportedVersion.GP_VERSION_7.isLE(loader.getVersion())) {
            builder
                    .column("res.collprovider")
                    .column("res.collisdeterministic");
        }

        if (PgSupportedVersion.VERSION_17.isLE(loader.getVersion())) {
            builder.column("res.colllocale");
        } else if (PgSupportedVersion.VERSION_15.isLE(loader.getVersion())) {
            builder.column("res.colliculocale");
        }

        if (PgSupportedVersion.VERSION_16.isLE(loader.getVersion())) {
            builder.column("res.collicurules");
        }
    }
}
