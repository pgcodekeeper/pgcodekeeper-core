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
package org.pgcodekeeper.core.utils;

import java.io.Serializable;

/**
 * Immutable pair utility class for holding two related objects.
 * Objects of this class are unmodifiable.
 * Use {@link ModPair} and {@link #copyMod()} to get modifiable Pairs.
 *
 * @param <K> type of the first element
 * @param <V> type of the second element
 */
public class Pair<K, V> implements Serializable {

    private static final long serialVersionUID = 8641621982915379836L;

    protected K first;
    protected V second;

    /**
     * Creates a new immutable pair with specified elements.
     *
     * @param first  the first element
     * @param second the second element
     */
    public Pair(K first, V second) {
        this.first = first;
        this.second = second;
    }

    /**
     * Returns the first element of this pair.
     *
     * @return the first element
     */
    public K getFirst() {
        return first;
    }

    /**
     * Returns the second element of this pair.
     *
     * @return the second element
     */
    public V getSecond() {
        return second;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((first == null) ? 0 : first.hashCode());
        result = prime * result + ((second == null) ? 0 : second.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Pair<?, ?> other = (Pair<?, ?>) obj;
        if (first == null) {
            if (other.first != null) {
                return false;
            }
        } else if (!first.equals(other.first)) {
            return false;
        }
        if (second == null) {
            return other.second == null;
        }
        return second.equals(other.second);
    }

    @Override
    public String toString() {
        return "(" + first + " - " + second + ")";
    }

    /**
     * Creates an immutable copy of this pair.
     *
     * @return new immutable Pair with same elements
     */
    public Pair<K, V> copy() {
        return new Pair<>(first, second);
    }

    /**
     * Creates a modifiable copy of this pair.
     *
     * @return new ModPair with same elements
     */
    public ModPair<K, V> copyMod() {
        return new ModPair<>(first, second);
    }
}
