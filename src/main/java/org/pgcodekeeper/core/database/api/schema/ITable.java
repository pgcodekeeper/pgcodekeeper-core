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
package org.pgcodekeeper.core.database.api.schema;

import org.pgcodekeeper.core.script.SQLScript;
import org.pgcodekeeper.core.settings.ISettings;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

/**
 * Interface for database table
 */
public interface ITable extends IRelation, IStatementContainer {

    IColumn getColumn(String name);

    @Override
    default DbObjType getStatementType() {
        return DbObjType.TABLE;
    }

    Collection<IColumn> getColumns();

    /**
     * Creates a stream that includes the statement itself and its columns if it's a table.
     *
     * @param st the statement to process
     * @return a stream containing the statement and its columns (if applicable)
     */
    static Stream<? extends IStatement> columnAdder(IStatement st) {
        Stream<IStatement> newStream = Stream.of(st);
        if (st instanceof ITable table) {
            newStream = Stream.concat(newStream, table.getColumns().stream());
        }

        return newStream;
    }

    /**
     * Adds commands to the script for move data from the temporary table to the new table, given the identity columns,
     * and a command to delete the temporary table.
     */
    void appendMoveDataSql(IStatement newCondition, SQLScript script, String tblTmpBareName,
                                  List<String> identityCols);
    /**
     * Compares this table with the {@code newTable} to determine if a full table recreation is required.
     * A full recreation (DROP and CREATE) is needed when the tables differ in ways that cannot
     * be altered using ALTER TABLE statements.
     *
     * @param newTable the new table definition to compare against
     * @param settings application settings that may affect the comparison logic
     * @return {@code true} if the table requires recreation (DROP and CREATE) rather than
     * being alterable, {@code false} if the changes can be applied via ALTER TABLE
     */
    boolean isRecreated(ITable newTable, ISettings settings);

    /**
     *
     * @param newTable new state of the table
     * @return true if the tables are identical
     */
    boolean compareIgnoringColumnOrder(ITable newTable);
}
