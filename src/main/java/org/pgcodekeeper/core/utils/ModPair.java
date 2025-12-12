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

import java.io.Serial;

/**
 * Modifiable pair class extending the immutable Pair class.
 * Provides setter methods to modify the first and second elements after construction.
 *
 * @param <K> type of the first element
 * @param <V> type of the second element
 */
public class ModPair<K, V> extends Pair<K, V> {

    @Serial
    private static final long serialVersionUID = 7623190777562696369L;

    /**
     * Creates a new modifiable pair with specified elements.
     *
     * @param first  the first element
     * @param second the second element
     */
    public ModPair(K first, V second) {
        super(first, second);
    }

    /**
     * Sets the first element of this pair.
     *
     * @param first the new first element
     */
    public void setFirst(K first) {
        this.first = first;
    }

    /**
     * Sets the second element of this pair and returns the previous value.
     *
     * @param second the new second element
     * @return the previous second element
     */
    public V setSecond(V second) {
        V old = this.second;
        this.second = second;
        return old;
    }
}
