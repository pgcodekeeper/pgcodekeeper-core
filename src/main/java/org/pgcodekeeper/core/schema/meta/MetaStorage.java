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
package org.pgcodekeeper.core.schema.meta;

import org.pgcodekeeper.core.Consts;
import org.pgcodekeeper.core.Utils;
import org.pgcodekeeper.core.loader.pg.JdbcSystemLoader;
import org.pgcodekeeper.core.loader.UrlJdbcConnector;
import org.pgcodekeeper.core.loader.pg.SupportedPgVersion;
import org.pgcodekeeper.core.localizations.Messages;
import org.pgcodekeeper.core.monitor.NullMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Storage and management system for database metadata objects.
 * Handles serialization, deserialization, and caching of system objects
 * for different PostgreSQL versions.
 */
public final class MetaStorage implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(MetaStorage.class);

    private static final long serialVersionUID = 8194906497159326596L;

    private static final String FILTER_PATTERN = """
        maxdepth=7;\
        org.pgcodekeeper.core.schema.meta.MetaStorage;\
        org.pgcodekeeper.core.schema.meta.MetaCast;\
        org.pgcodekeeper.core.schema.meta.MetaContainer;\
        org.pgcodekeeper.core.schema.meta.MetaFunction;\
        org.pgcodekeeper.core.schema.meta.MetaOperator;\
        org.pgcodekeeper.core.schema.meta.MetaRelation;\
        org.pgcodekeeper.core.schema.meta.MetaStatement;\
        org.pgcodekeeper.core.schema.PgObjLocation;\
        org.pgcodekeeper.core.schema.PgObjLocation$LocationType;\
        org.pgcodekeeper.core.schema.Argument;\
        org.pgcodekeeper.core.schema.ArgMode;\
        org.pgcodekeeper.core.schema.GenericColumn;\
        org.pgcodekeeper.core.schema.ICast$CastContext;\
        org.pgcodekeeper.core.DangerStatement;\
        org.pgcodekeeper.core.model.difftree.DbObjType;\
        org.pgcodekeeper.core.ContextLocation;\
        org.pgcodekeeper.core.utils.Pair;\
        java.util.ArrayList;\
        java.lang.Object;\
        java.lang.Enum;\
        !*""";

    private static final ObjectInputFilter DESERIALIZATION_FILTER = ObjectInputFilter.Config.createFilter(FILTER_PATTERN);

    /**
     * Base file name for serialized system objects.
     */
    public static final String FILE_NAME = "SYSTEM_OBJECTS_";

    private static final ConcurrentMap<SupportedPgVersion, MetaStorage> STORAGE_CACHE = new ConcurrentHashMap<>();

    private final List<MetaStatement> definitions = new ArrayList<>();

    /**
     * Adds a metadata statement to this storage.
     *
     * @param meta the metadata statement to add
     */
    public void addMetaChild(MetaStatement meta) {
        definitions.add(meta);
    }

    /**
     * Returns system objects for the specified PostgreSQL version.
     *
     * @param version the PostgreSQL version
     * @return list of system metadata objects
     */
    static List<MetaStatement> getSystemObjects(SupportedPgVersion version) {
        MetaStorage storage = getObjectsFromResources(version);
        return storage != null ? storage.definitions : Collections.emptyList();
    }

    private static MetaStorage getObjectsFromResources(SupportedPgVersion ver) {
        MetaStorage db = STORAGE_CACHE.get(ver);
        if (db != null) {
            return db;
        }

        InputStream inputStream = MetaStorage.class.getResourceAsStream(FILE_NAME + ver + ".ser");

        MetaStorage object = deserialize(inputStream);
        if (object != null) {
            STORAGE_CACHE.putIfAbsent(ver, object);
        }

        return object;
    }

    /**
     * Deserializes object
     *
     * @param inputStream - stream of serialized file
     * @return deserialized object or null if not found
     */
    private static MetaStorage deserialize(InputStream inputStream) {
        if (inputStream == null) {
            return null;
        }

        try (ObjectInputStream oin = new ObjectInputStream(inputStream)) {
            oin.setObjectInputFilter(DESERIALIZATION_FILTER);
            return (MetaStorage) oin.readObject();
        } catch (ClassNotFoundException | IOException e) {
            LOG.debug(Messages.Utils_log_err_deserialize, e);
        }
        return null;
    }

    /**
     * Serializes system objects from a database connection to a file.
     *
     * @param path the output file path
     * @param url  the database connection URL
     * @throws IOException          if an I/O error occurs
     * @throws InterruptedException if the operation is interrupted
     */
    public static void serialize(String path, String url) throws IOException, InterruptedException {
        UrlJdbcConnector jdbcConnector = new UrlJdbcConnector(url);
        Serializable storage = new JdbcSystemLoader(jdbcConnector, Consts.UTC,
                new NullMonitor()).getStorageFromJdbc();
        Utils.serialize(path, storage);
    }
}
