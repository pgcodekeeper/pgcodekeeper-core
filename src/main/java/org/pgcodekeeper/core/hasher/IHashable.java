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

/**
 * Interface for objects that can compute their own hash value using a {@link Hasher}.
 */
public interface IHashable {
    /**
     * Computes the hash of the implementing object using the provided hasher.
     * The implementation should call appropriate {@code put} methods on the hasher
     * for all fields that should contribute to the hash value.
     *
     * @param hasher the hasher instance to use for hash computation
     */
    void computeHash(Hasher hasher);
}
