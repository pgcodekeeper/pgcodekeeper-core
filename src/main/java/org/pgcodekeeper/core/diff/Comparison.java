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
package org.pgcodekeeper.core.diff;

import org.pgcodekeeper.core.model.difftree.DbObjType;
import org.pgcodekeeper.core.schema.AbstractTable;
import org.pgcodekeeper.core.schema.PgStatement;
import org.pgcodekeeper.core.settings.ISettings;

/**
 * Class with common logic for comparing objects.
 */
public class Comparison {

    /**
     * Compares old and new states of an object
     *
     * @param settings settings for comparing objects
     * @param oldObject old object state
     * @param newObject new object state
     * @return true if objects are equals
     */
    public static boolean compare(ISettings settings, PgStatement oldObject, PgStatement newObject) {
        if (oldObject.hashCode() == newObject.hashCode() && oldObject.equals(newObject)) {
            return true;
        }

        if (DbObjType.TABLE == oldObject.getStatementType() && settings.isIgnoreColumnOrder()) {
            return AbstractTable.compareIgnoringColumnOrder((AbstractTable) oldObject, (AbstractTable) newObject)
                    && oldObject.compareChildren(newObject);
        }

        return false;
    }

    private Comparison() {
        // only statics
    }
}
