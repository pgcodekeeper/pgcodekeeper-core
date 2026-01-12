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
package org.pgcodekeeper.core.api;

import java.util.Collection;
import java.util.Collections;
import org.pgcodekeeper.core.database.api.IDatabaseProvider;
import org.pgcodekeeper.core.database.ch.ChDatabaseProvider;
import org.pgcodekeeper.core.database.ms.MsDatabaseProvider;
import org.pgcodekeeper.core.database.pg.PgDatabaseProvider;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Global registry for managing {@link IDatabaseProvider} implementations by their unique names.
 *
 * <p>This registry uses a {@link ConcurrentHashMap} for thread-safe access,
 * allowing efficient registration, lookup, and removal by name.</p>
 */
public final class ApiRegistry {

    private static final Map<String, IDatabaseProvider> INSTANCES = new ConcurrentHashMap<>();

    private ApiRegistry() {
        throw new UnsupportedOperationException("Utility class");
    }

    // Static initializer for default implementations
    static {
        register(new PgDatabaseProvider());
        register(new MsDatabaseProvider());
        register(new ChDatabaseProvider());
    }

    /**
     * Registers a new API implementation using its name as the key.
     * If an implementation with the same name already exists, it will be overwritten.
     *
     * @param implementation the implementation to register; must not be null.
     * @throws IllegalArgumentException if implementation is null or name is null/empty.
     */
    public static void register(IDatabaseProvider implementation) {
        if (implementation == null) {
            throw new IllegalArgumentException("Implementation cannot be null");
        }
        String name = implementation.getName();
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Implementation name must not be null or empty");
        }
        INSTANCES.put(name, implementation);
    }

    /**
     * Retrieves an implementation by its unique name.
     *
     * @param name the name of the implementation.
     * @return implementation, or null if not found.
     */
    public static IDatabaseProvider get(String name) {
        return INSTANCES.get(name);
    }

    /**
     * Unregisters an implementation by its name.
     *
     * @param name the name of the implementation to remove.
     * @return the removed implementation, or null if none was registered under this name.
     */
    public static IDatabaseProvider unregister(String name) {
        return INSTANCES.remove(name);
    }

    /**
     * Returns an unmodifiable collection of all registered implementations.
     *
     * @return a read-only view of the registered implementations.
     */
    public static Collection<IDatabaseProvider> getImplementations() {
        return Collections.unmodifiableCollection(INSTANCES.values());
    }
}