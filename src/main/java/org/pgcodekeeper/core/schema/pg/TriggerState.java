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
package org.pgcodekeeper.core.schema.pg;

/**
 * Enumeration of PostgreSQL trigger states.
 * Represents the various states a trigger can be in, controlling
 * when and how the trigger fires in relation to replication and system events.
 */
public enum TriggerState {
    ENABLE("ENABLE"),
    ENABLE_ALWAYS("ENABLE ALWAYS"),
    ENABLE_REPLICA("ENABLE REPLICA"),
    DISABLE("DISABLE");

    private final String value;

    /**
     * Gets the SQL string representation of this trigger state.
     *
     * @return SQL trigger state string
     */
    public String getValue() {
        return value;
    }

    TriggerState(String value) {
        this.value = value;
    }
}
