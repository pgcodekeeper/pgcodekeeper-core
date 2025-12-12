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
package org.pgcodekeeper.core.database.api.schema;

/**
 * Represents an override of a database statement when loading from multiple sources.
 * Contains both the original statement and the new statement that overrides it,
 * along with location information for tracking where each version came from.
 *
 * @param newStatement the new statement that overrides the old one
 * @param oldStatement the original statement being overridden
 */
public record ObjectOverride(IStatement newStatement, IStatement oldStatement) {

    /**
     * Gets the name of the overridden statement.
     *
     * @return the statement name
     */
    public String getName() {
        return newStatement.getName();
    }

    /**
     * Gets the type of the overridden statement.
     *
     * @return the statement type
     */
    public DbObjType getType() {
        return newStatement.getStatementType();
    }

    /**
     * Gets the file path where the new statement is defined.
     *
     * @return the file path of the new statement
     */
    public String getNewPath() {
        return getStatementPath(newStatement);
    }

    /**
     * Gets the file path where the old statement is defined.
     *
     * @return the file path of the old statement
     */
    public String getOldPath() {
        return getStatementPath(oldStatement);
    }

    private String getStatementPath(IStatement st) {
        ObjectLocation loc = st.getLocation();
        if (loc != null) {
            return loc.getFilePath();
        }
        if (st.isLib()) {
            return st.getLibName();
        }
        return null;
    }

    /**
     * Gets the location information for the new statement.
     *
     * @return location of the new statement
     */
    public ObjectLocation getNewLocation() {
        return newStatement.getLocation();
    }

    /**
     * Gets the location information for the old statement.
     *
     * @return location of the old statement
     */
    public ObjectLocation getOldLocation() {
        return oldStatement.getLocation();
    }
}
