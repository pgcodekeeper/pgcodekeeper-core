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
package org.pgcodekeeper.core;

import java.util.EnumSet;
import java.util.Set;

/**
 * Enumeration of potentially dangerous SQL statements that should be handled with caution.
 * Used to control which dangerous operations are allowed during database operations.
 */
public enum DangerStatement {
    DROP_TABLE,
    ALTER_COLUMN,
    DROP_COLUMN,
    RESTART_WITH,
    UPDATE;

    /**
     * Creates a set of dangerous statements that should be considered "allowed"
     * based on the provided ignore flags.
     *
     * @return EnumSet containing all allowed dangerous statements
     */
    public static Set<DangerStatement> getAllowedDanger(boolean ignoreDropCol, boolean ignoreAlterCol,
            boolean ignoreDropTable, boolean ignoreRestartWith, boolean ignoreUpdate) {
        Set<DangerStatement> allowedDangers = EnumSet.noneOf(DangerStatement.class);
        if (ignoreDropCol) {
            allowedDangers.add(DangerStatement.DROP_COLUMN);
        }
        if (ignoreAlterCol) {
            allowedDangers.add(DangerStatement.ALTER_COLUMN);
        }
        if (ignoreDropTable) {
            allowedDangers.add(DangerStatement.DROP_TABLE);
        }
        if (ignoreRestartWith) {
            allowedDangers.add(DangerStatement.RESTART_WITH);
        }
        if (ignoreUpdate) {
            allowedDangers.add(DangerStatement.UPDATE);
        }

        return allowedDangers;
    }
}