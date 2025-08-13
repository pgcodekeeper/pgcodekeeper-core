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
package org.pgcodekeeper.core.hasher;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Interface defining methods for hashing various types of data.
 * Implementations should provide consistent hashing for all supported types.
 */
public interface Hasher {
    /**
     * Adds a primitive boolean value to the hash computation
     */
    void put(boolean b);

    /**
     * Adds a Boolean object to the hash computation
     */
    void put(Boolean b);

    /**
     * Adds a String value to the hash computation
     */
    void put(String s);

    /**
     * Adds a primitive float value to the hash computation
     */
    void put(float f);

    /**
     * Adds a primitive int value to the hash computation
     */
    void put(int i);

    /**
     * Adds an Integer object to the hash computation
     */
    void put(Integer i);

    /**
     * Adds a hashable object to the hash computation
     *
     * @param hashable object implementing IHashable interface
     */
    void put(IHashable hashable);

    /**
     * Adds an enum value to the hash computation
     */
    void put(Enum<?> en);

    /**
     * Adds a Map of String key-value pairs to the hash computation
     */
    void put(Map<String, String> map);

    /**
     * Adds a List of Strings to the hash computation (order-sensitive)
     */
    void put(List<String> col);

    /**
     * Adds a Set of Strings to the hash computation (order-insensitive)
     */
    void put(Set<String> col);

    /**
     * Adds a Collection of hashable objects with order sensitivity
     *
     * @param col collection of objects implementing IHashable
     */
    void putOrdered(Collection<? extends IHashable> col);

    /**
     * Adds a Collection of hashable objects with order insensitivity
     *
     * @param col collection of objects implementing IHashable
     */
    void putUnordered(Collection<? extends IHashable> col);

    /**
     * Adds a Map of hashable objects with order insensitivity
     *
     * @param map map containing objects implementing IHashable
     */
    void putUnordered(Map<String, ? extends IHashable> map);
}