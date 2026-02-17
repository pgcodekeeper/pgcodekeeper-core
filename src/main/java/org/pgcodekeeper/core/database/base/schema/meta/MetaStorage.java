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
package org.pgcodekeeper.core.database.base.schema.meta;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import org.pgcodekeeper.core.database.api.jdbc.ISupportedVersion;
import org.pgcodekeeper.core.localizations.Messages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Storage and management system for database metadata objects.
 * Handles serialization, deserialization, and caching of system objects
 * for different PostgreSQL versions.
 */
public final class MetaStorage implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(MetaStorage.class);

    @Serial
    private static final long serialVersionUID = -277642492286966398L;

    private static final String FILTER_PATTERN = """
            maxdepth=7;\
            org.pgcodekeeper.core.database.base.schema.meta.MetaStorage;\
            org.pgcodekeeper.core.database.base.schema.meta.MetaCast;\
            org.pgcodekeeper.core.database.base.schema.meta.MetaContainer;\
            org.pgcodekeeper.core.database.base.schema.meta.MetaFunction;\
            org.pgcodekeeper.core.database.base.schema.meta.MetaOperator;\
            org.pgcodekeeper.core.database.base.schema.meta.MetaRelation;\
            org.pgcodekeeper.core.database.base.schema.meta.MetaStatement;\
            org.pgcodekeeper.core.database.api.schema.ObjectLocation;\
            org.pgcodekeeper.core.database.api.schema.ObjectLocation$LocationType;\
            org.pgcodekeeper.core.database.api.schema.ObjectReference;\
            org.pgcodekeeper.core.database.api.schema.DbObjType;\
            org.pgcodekeeper.core.database.api.schema.ArgMode;\
            org.pgcodekeeper.core.database.api.schema.ICast$CastContext;\
            org.pgcodekeeper.core.database.base.schema.Argument;\
            org.pgcodekeeper.core.DangerStatement;\
            org.pgcodekeeper.core.ContextLocation;\
            org.pgcodekeeper.core.utils.Pair;\
            java.util.ArrayList;\
            java.util.Map$Entry;\
            java.util.HashMap;\
            java.util.LinkedHashMap;\
            java.lang.Object;\
            java.lang.Enum;\
            !*""";

    private static final ObjectInputFilter DESERIALIZATION_FILTER = ObjectInputFilter.Config.createFilter(FILTER_PATTERN);

    /**
     * Base file name for serialized system objects.
     */
    public static final String FILE_NAME = "SYSTEM_OBJECTS_";

    private static final ConcurrentMap<ISupportedVersion, MetaStorage> STORAGE_CACHE = new ConcurrentHashMap<>();

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
    static List<MetaStatement> getSystemObjects(ISupportedVersion version) {
        MetaStorage storage = getObjectsFromResources(version);
        return storage != null ? storage.definitions : Collections.emptyList();
    }

    private static MetaStorage getObjectsFromResources(ISupportedVersion ver) {
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
}
