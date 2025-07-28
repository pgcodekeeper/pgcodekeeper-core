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
package org.pgcodekeeper.core.schema;

/**
 * Enumeration of possible states for database objects during schema comparison and migration.
 * Defines what action needs to be taken for an object when applying schema changes.
 */
public enum ObjectState {
    CREATE,
    RECREATE,
    ALTER,
    DROP,
    ALTER_WITH_DEP,
    NOTHING;

    /**
     * Checks if this state matches any of the provided states.
     *
     * @param states the states to check against
     * @return true if this state is in the provided list
     */
    public boolean in(ObjectState... states) {
        for (ObjectState state : states) {
            if (this == state) {
                return true;
            }
        }
        return false;
    }
}
