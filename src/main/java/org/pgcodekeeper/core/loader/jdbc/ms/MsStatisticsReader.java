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
package org.pgcodekeeper.core.loader.jdbc.ms;

import org.pgcodekeeper.core.loader.QueryBuilder;
import org.pgcodekeeper.core.loader.jdbc.JdbcLoaderBase;
import org.pgcodekeeper.core.loader.jdbc.JdbcReader;
import org.pgcodekeeper.core.loader.jdbc.XmlReader;
import org.pgcodekeeper.core.loader.jdbc.XmlReaderException;
import org.pgcodekeeper.core.loader.ms.SupportedMsVersion;
import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.schema.AbstractSchema;
import org.pgcodekeeper.core.schema.GenericColumn;
import org.pgcodekeeper.core.schema.ms.MsStatistics;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Reader for Microsoft SQL statistics.
 * Loads statistics definitions from sys.stats system view.
 */
public final class MsStatisticsReader extends JdbcReader {

    /**
     * Constructs a new Microsoft SQL statistics reader.
     *
     * @param loader the JDBC loader base for database operations
     */
    public MsStatisticsReader(JdbcLoaderBase loader) {
        super(loader);
    }

    @Override
    protected void processResult(ResultSet res, AbstractSchema schema) throws SQLException, XmlReaderException {
        var name = res.getString("name");
        var stat = new MsStatistics(name);
        var schemaName = schema.getName();
        var containerName = res.getString("cont_name");
        loader.setCurrentObject(new GenericColumn(schemaName, containerName, name, DbObjType.STATISTICS));

        for (var col : XmlReader.readXML(res.getString("cols"))) {
            var colName = col.getString("name");
            stat.addCol(colName);
            stat.addDep(new GenericColumn(schemaName, containerName, colName, DbObjType.COLUMN));
        }

        var filter = res.getString("filter_definition");
        if (filter != null) {
            stat.setFilter(filter);
        }
        if (res.getBoolean("no_recompute")) {
            stat.putOption("NORECOMPUTE", "NORECOMPUTE");
        }

        if (res.getBoolean("is_incremental")) {
            stat.putOption("INCREMENTAL", "ON");
        }

        if (SupportedMsVersion.VERSION_22.isLE(loader.getVersion())) {
            var sample = res.getInt("persisted_sample_percent");
            if (sample != 0) {
                stat.putOption("SAMPLE", Integer.toString(sample) + " PERCENT");
            }
            if (res.getBoolean("auto_drop")) {
                stat.putOption("AUTO_DROP", "ON");
            }
        }

        schema.getStatementContainer(containerName).addChild(stat);
    }

    @Override
    protected void fillQueryBuilder(QueryBuilder builder) {
        QueryBuilder subSelect = new QueryBuilder()
                .column("s_column.name AS name")
                .from("sys.stats_columns st_col WITH (NOLOCK)")
                .join("LEFT JOIN sys.columns s_column WITH (NOLOCK) ON s_column.object_id = res.object_id AND s_column.column_id = st_col.column_id")
                .where("st_col.object_id = res.object_id")
                .where("st_col.stats_id = res.stats_id")
                .orderBy("st_col.stats_column_id")
                .postAction("FOR XML RAW, ROOT");

        builder
                .column("res.name")
                .column("cont.name AS cont_name")
                .column("cols")
                .column("res.filter_definition")
                .column("res.no_recompute")
                .column("res.is_incremental")
                .from("sys.stats res WITH (NOLOCK)")
                .join("CROSS APPLY", subSelect, "st_col (cols)")
                .join("LEFT JOIN sys.objects cont WITH (NOLOCK) ON cont.object_id = res.object_id")
                .where("res.user_created = 1");

        if (SupportedMsVersion.VERSION_22.isLE(loader.getVersion())) {
            builder
                    .column("res.auto_drop")
                    .column("st_prop.persisted_sample_percent")
                    .join("CROSS APPLY sys.dm_db_stats_properties(res.object_id, res.stats_id) st_prop");
        }
    }

    @Override
    protected String getSchemaColumn() {
        return "cont.schema_id";
    }
}