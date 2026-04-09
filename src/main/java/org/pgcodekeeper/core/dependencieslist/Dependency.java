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
package org.pgcodekeeper.core.dependencieslist;

import org.pgcodekeeper.core.database.api.schema.*;

/**
 * A record representing a directed dependency between two object references.
 * <p>
 * Interpreted as: {@code source → target}
 * (source depends on target).
 * <p>
 * A dependency can be either <strong>strong</strong> or <strong>weak</strong>:
 * <ul>
 *   <li>Strong dependency — the source cannot function without the target
 *       (e.g., composition, inheritance, required import)</li>
 *   <li>Weak dependency — the source may optionally use the target
 *       (e.g., observer, soft reference, suggested but not required)</li>
 * </ul>
 * <p>
 * This class does not validate the references nor guarantee that the
 * dependency is acyclic — that is the responsibility of the calling code.
 *
 * @param source  the dependent object (outgoing arrow)
 * @param target  the object providing the dependency (incoming arrow)
 * @param isStrong   {@code true} for strong dependency, {@code false} for weak;
 *                   defaults to {@code true} for forward compatibility
 */
public final record Dependency(ObjectReference source, ObjectReference target, boolean isStrong) {

    /**
     * Canonical constructor with isStrong defaulting to {@code true}.
     * <p>
     * This constructor is provided for backward compatibility and convenience.
     *
     * @param source the dependent object
     * @param target the object being depended upon
     */
    public Dependency(ObjectReference source, ObjectReference target) {
        this(source, target, true);
    }
  }
